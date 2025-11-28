import type { AddedHandler, ImmutableProps, ModifiedHandler, RemovedHandler, Step, TypeDescriptor } from '../pipeline';

/**
 * Computes a hash key for a path (for map lookups).
 */
function computePathHash(path: string[]): string {
    return path.join('::');
}

/**
 * A step that computes the minimum or maximum value of a property over items in a nested array.
 * 
 * - Returns undefined for empty arrays
 * - Ignores null/undefined values in comparison
 * - Handles removal by tracking all values and recalculating
 */
export class MinMaxAggregateStep<
    TInput,
    TPath extends string[],
    TPropertyName extends string
> implements Step {
    
    /** Maps parent path hash to array of numeric values (excluding null/undefined) */
    private valueStore: Map<string, number[]> = new Map();
    
    /** Handlers for modified events at various levels */
    private modifiedHandlers: Array<{
        pathNames: string[];
        handler: ModifiedHandler;
    }> = [];
    
    constructor(
        private input: Step,
        private arrayPath: TPath,
        private propertyName: TPropertyName,
        private numericProperty: string,
        private aggregateFn: (values: number[]) => number
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
            // This is the channel for receiving aggregate values
            this.modifiedHandlers.push({
                pathNames,
                handler
            });
        }
        // Always pass through to input for other property modifications
        this.input.onModified(pathNames, handler);
    }
    
    /**
     * Checks if the given path represents the parent level (where aggregate property lives)
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
        
        // Extract numeric value (ignore null/undefined)
        const value = item[this.numericProperty];
        if (value !== null && value !== undefined) {
            const numValue = Number(value);
            if (!isNaN(numValue)) {
                // Add to value store
                const values = this.valueStore.get(parentHash) || [];
                values.push(numValue);
                this.valueStore.set(parentHash, values);
            }
        }
        
        // Compute new aggregate using the provided function
        const values = this.valueStore.get(parentHash) || [];
        const newAggregate = values.length > 0 ? this.aggregateFn(values) : undefined;
        
        // Emit modification event
        if (parentPath.length > 0) {
            const parentKey = parentPath[parentPath.length - 1];
            const pathToParent = parentPath.slice(0, -1);
            
            this.modifiedHandlers.forEach(({ handler }) => {
                handler(pathToParent, parentKey, this.propertyName, newAggregate);
            });
        } else {
            // Parent is at root level
            this.modifiedHandlers.forEach(({ handler }) => {
                handler([], '', this.propertyName, newAggregate);
            });
        }
    }
    
    /**
     * Handle when an item is removed from the target array
     */
    private handleItemRemoved(runtimePath: string[], itemKey: string, item: ImmutableProps): void {
        const parentPath = runtimePath;
        const parentHash = computePathHash(parentPath);
        
        // Remove value from store if it was numeric
        const value = item[this.numericProperty];
        if (value !== null && value !== undefined) {
            const numValue = Number(value);
            if (!isNaN(numValue)) {
                const values = this.valueStore.get(parentHash);
                if (values) {
                    const index = values.indexOf(numValue);
                    if (index >= 0) {
                        values.splice(index, 1);
                        if (values.length === 0) {
                            this.valueStore.delete(parentHash);
                        } else {
                            this.valueStore.set(parentHash, values);
                        }
                    }
                }
            }
        }
        
        // Compute new aggregate using the provided function
        const values = this.valueStore.get(parentHash) || [];
        const newAggregate = values.length > 0 ? this.aggregateFn(values) : undefined;
        
        // Emit modification event
        if (parentPath.length > 0) {
            const parentKey = parentPath[parentPath.length - 1];
            const pathToParent = parentPath.slice(0, -1);
            
            this.modifiedHandlers.forEach(({ handler }) => {
                handler(pathToParent, parentKey, this.propertyName, newAggregate);
            });
        } else {
            this.modifiedHandlers.forEach(({ handler }) => {
                handler([], '', this.propertyName, newAggregate);
            });
        }
    }
}
