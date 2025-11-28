import type { AddedHandler, ImmutableProps, ModifiedHandler, RemovedHandler, Step, TypeDescriptor } from '../pipeline';

/**
 * Computes a hash key for a path (for map lookups).
 */
function computePathHash(path: string[]): string {
    return path.join('::');
}

/**
 * Compares two values, supporting both numeric and string comparisons.
 * Returns true if value1 < value2.
 */
function compareValues(value1: number | string, value2: number | string): boolean {
    // If both are numbers, use numeric comparison
    if (typeof value1 === 'number' && typeof value2 === 'number') {
        return value1 < value2;
    }
    // If both are strings, use lexicographic comparison
    if (typeof value1 === 'string' && typeof value2 === 'string') {
        return value1 < value2;
    }
    // Mixed types: convert to string for comparison
    return String(value1) < String(value2);
}

/**
 * Determines if a value is numeric.
 */
function isNumeric(value: any): value is number {
    if (typeof value === 'number') {
        return !isNaN(value);
    }
    const numValue = Number(value);
    return !isNaN(numValue) && isFinite(numValue);
}

/**
 * A step that picks the object with the minimum or maximum value of a property from a nested array.
 * 
 * - Returns undefined for empty arrays
 * - Ignores null/undefined values in comparison
 * - Handles removal by tracking all items and recalculating
 * - Supports both numeric and string comparisons
 */
export class PickByMinMaxStep<
    TInput,
    TPath extends string[],
    TPropertyName extends string
> implements Step {
    
    /** Maps item path hash to item data (needed for recalculation when picked item is removed) */
    private itemStore: Map<string, ImmutableProps> = new Map();
    
    /** Maps parent path hash to current min/max item */
    private pickedItemStore: Map<string, ImmutableProps> = new Map();
    
    /** Maps parent path hash to comparison value of current picked item */
    private comparisonValueStore: Map<string, number | string> = new Map();
    
    /** Handlers for modified events at various levels */
    private modifiedHandlers: Array<{
        pathNames: string[];
        handler: ModifiedHandler;
    }> = [];
    
    constructor(
        private input: Step,
        private arrayPath: TPath,
        private propertyName: TPropertyName,
        private comparisonProperty: string,
        private compareFn: (value1: number | string, value2: number | string) => boolean
    ) {
        // Register with input step to receive item add/remove events at the target array level
        this.input.onAdded(this.arrayPath, (path, key, immutableProps) => {
            this.handleItemAdded(path, key, immutableProps);
        });
        
        this.input.onRemoved(this.arrayPath, (path, key, immutableProps) => {
            this.handleItemRemoved(path, key, immutableProps);
        });
    }
    
    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }
    
    onAdded(pathNames: string[], handler: AddedHandler): void {
        this.input.onAdded(pathNames, handler);
    }
    
    onRemoved(pathNames: string[], handler: RemovedHandler): void {
        this.input.onRemoved(pathNames, handler);
    }
    
    onModified(pathNames: string[], handler: ModifiedHandler): void {
        if (this.isParentPath(pathNames)) {
            // Handler wants modification events at parent level
            // This is the channel for receiving picked object
            this.modifiedHandlers.push({
                pathNames,
                handler
            });
        }
        // Always pass through to input for other property modifications
        this.input.onModified(pathNames, handler);
    }
    
    /**
     * Checks if the given path represents the parent level (where picked object property lives)
     */
    private isParentPath(pathNames: string[]): boolean {
        // Parent path is arrayPath without the last element
        const parentPath = this.arrayPath.slice(0, -1);
        
        if (pathNames.length !== parentPath.length) {
            return false;
        }
        
        return pathNames.every((name, i) => name === parentPath[i]);
    }
    
    /**
     * Handle when an item is added to the target array
     */
    private handleItemAdded(runtimePath: string[], itemKey: string, item: ImmutableProps): void {
        const parentPath = runtimePath;
        const parentHash = computePathHash(parentPath);
        const itemPath = [...runtimePath, itemKey];
        const itemHash = computePathHash(itemPath);
        
        // Store item for later removal
        this.itemStore.set(itemHash, item);
        
        // Extract comparison value (ignore null/undefined)
        const value = item[this.comparisonProperty];
        if (value === null || value === undefined) {
            // Ignore null/undefined values
            // If we don't have a picked item yet, emit undefined
            if (!this.pickedItemStore.has(parentHash)) {
                this.emitModification(parentPath, undefined);
            }
            return;
        }
        
        // Determine if numeric or string
        const comparisonValue: number | string = isNumeric(value) ? Number(value) : String(value);
        
        // Check if this is a new min/max based on comparison function
        const currentPickedValue = this.comparisonValueStore.get(parentHash);
        let shouldUpdate = false;
        
        if (currentPickedValue === undefined) {
            // No current picked item, this becomes the picked item
            shouldUpdate = true;
        } else if (this.compareFn(comparisonValue, currentPickedValue)) {
            // New value is better (smaller for min, larger for max), update picked item
            shouldUpdate = true;
        }
        
        if (shouldUpdate) {
            this.pickedItemStore.set(parentHash, item);
            this.comparisonValueStore.set(parentHash, comparisonValue);
            this.emitModification(parentPath, item);
        }
    }
    
    /**
     * Handle when an item is removed from the target array
     */
    private handleItemRemoved(runtimePath: string[], itemKey: string, item: ImmutableProps): void {
        const parentPath = runtimePath;
        const parentHash = computePathHash(parentPath);
        const itemPath = [...runtimePath, itemKey];
        const itemHash = computePathHash(itemPath);
        
        // Remove from tracking (needed for recalculation)
        this.itemStore.delete(itemHash);
        
        // Check if the removed item was the current picked item
        const currentPickedItem = this.pickedItemStore.get(parentHash);
        const isRemovedItemPicked = currentPickedItem && this.itemsEqual(item, currentPickedItem);
        
        if (isRemovedItemPicked) {
            // Need to recalculate picked item from remaining items
            this.recalculatePickedItem(parentPath, parentHash);
        }
    }
    
    /**
     * Recalculates the picked item (min or max) from all remaining items for a given parent.
     */
    private recalculatePickedItem(parentPath: string[], parentHash: string): void {
        // Find all items for this parent
        // Item hash format: parentHash::itemKey
        const parentPrefix = parentHash + '::';
        const itemsForParent: Array<{ item: ImmutableProps; value: number | string }> = [];
        
        for (const [itemHash, item] of this.itemStore.entries()) {
            // Check if this item belongs to this parent
            if (itemHash.startsWith(parentPrefix)) {
                const value = item[this.comparisonProperty];
                if (value !== null && value !== undefined) {
                    const comparisonValue: number | string = isNumeric(value) ? Number(value) : String(value);
                    itemsForParent.push({ item, value: comparisonValue });
                }
            }
        }
        
        if (itemsForParent.length === 0) {
            // No remaining items with valid values
            this.pickedItemStore.delete(parentHash);
            this.comparisonValueStore.delete(parentHash);
            this.emitModification(parentPath, undefined);
        } else {
            // Find the picked item (min or max) - first encountered wins on ties
            let pickedItem = itemsForParent[0].item;
            let pickedValue = itemsForParent[0].value;
            
            for (let i = 1; i < itemsForParent.length; i++) {
                if (this.compareFn(itemsForParent[i].value, pickedValue)) {
                    pickedItem = itemsForParent[i].item;
                    pickedValue = itemsForParent[i].value;
                }
            }
            
            this.pickedItemStore.set(parentHash, pickedItem);
            this.comparisonValueStore.set(parentHash, pickedValue);
            this.emitModification(parentPath, pickedItem);
        }
    }
    
    /**
     * Checks if two items are equal (shallow comparison).
     */
    private itemsEqual(item1: ImmutableProps, item2: ImmutableProps): boolean {
        const keys1 = Object.keys(item1);
        const keys2 = Object.keys(item2);
        
        if (keys1.length !== keys2.length) {
            return false;
        }
        
        for (const key of keys1) {
            if (item1[key] !== item2[key]) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Emits a modification event for the picked object.
     */
    private emitModification(parentPath: string[], pickedItem: ImmutableProps | undefined): void {
        if (parentPath.length > 0) {
            const parentKey = parentPath[parentPath.length - 1];
            const pathToParent = parentPath.slice(0, -1);
            
            this.modifiedHandlers.forEach(({ handler }) => {
                handler(pathToParent, parentKey, this.propertyName, pickedItem);
            });
        } else {
            // Parent is at root level
            this.modifiedHandlers.forEach(({ handler }) => {
                handler([], '', this.propertyName, pickedItem);
            });
        }
    }
}

