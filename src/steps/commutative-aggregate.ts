import type { AddedHandler, ImmutableProps, ModifiedHandler, RemovedHandler, Step, TypeDescriptor } from '../pipeline';

/**
 * Operator called when an item is added to the aggregated array.
 * 
 * @param currentAggregate - The current aggregate value, or undefined if this is the first item
 * @param item - The immutable properties of the item being added
 * @returns The new aggregate value
 */
export type AddOperator<TItem, TAggregate> = (
    currentAggregate: TAggregate | undefined,
    item: TItem
) => TAggregate;

/**
 * Operator called when an item is removed from the aggregated array.
 * 
 * @param currentAggregate - The current aggregate value (never undefined since an item must exist to be removed)
 * @param item - The immutable properties of the item being removed
 * @returns The new aggregate value
 */
export type SubtractOperator<TItem, TAggregate> = (
    currentAggregate: TAggregate,
    item: TItem
) => TAggregate;

/**
 * Configuration for the commutative aggregate operation.
 */
interface CommutativeAggregateConfig<TItem, TAggregate> {
    /** Operator called when an item is added */
    add: AddOperator<TItem, TAggregate>;
    
    /** Operator called when an item is removed */
    subtract: SubtractOperator<TItem, TAggregate>;
}

/**
 * Computes a hash key for a path (for map lookups).
 */
function computePathHash(path: string[]): string {
    return path.join('::');
}

/**
 * A step that computes an aggregate value over items in a nested array.
 *
 * This step:
 * 1. Registers for add/remove events at the target array level
 * 2. Maintains aggregate state keyed by parent path
 * 3. Emits onModified events when the aggregate changes
 * 4. Transforms the type by replacing the array with the aggregate property
 *
 * **IMPORTANT: Event Channel Separation**
 * - `onAdded`/`onRemoved` pass through immutable properties ONLY (no aggregate)
 * - `onModified` is the ONLY channel for communicating aggregate values
 * - This follows the system pattern: mutable/computed properties use onModified
 *
 * @template TInput - The input type to the step
 * @template TPath - The tuple of array names forming the path to the target array
 * @template TPropertyName - The name of the new aggregate property
 * @template TAggregate - The type of the aggregate value
 */
export class CommutativeAggregateStep<
    TInput,
    TPath extends string[],
    TPropertyName extends string,
    TAggregate
> implements Step {
    
    /** Maps parent path hash to current aggregate value */
    private aggregateValues: Map<string, TAggregate> = new Map();
    
    /** Maps item path hash to item data for removal lookup */
    private itemStore: Map<string, ImmutableProps> = new Map();
    
    /** Handlers for modified events at various levels */
    private modifiedHandlers: Array<{
        pathNames: string[];
        handler: ModifiedHandler;
    }> = [];
    
    constructor(
        private input: Step,
        private arrayPath: TPath,
        private propertyName: TPropertyName,
        private config: CommutativeAggregateConfig<ImmutableProps, TAggregate>
    ) {
        // Register with input step to receive item add/remove events at the target array level
        this.input.onAdded(this.arrayPath, (path, key, immutableProps) => {
            this.handleItemAdded(path, key, immutableProps);
        });
        
        this.input.onRemoved(this.arrayPath, (path, key) => {
            this.handleItemRemoved(path, key);
        });
    }
    
    getTypeDescriptor(): TypeDescriptor {
        const inputDescriptor = this.input.getTypeDescriptor();
        return this.transformDescriptor(inputDescriptor, [...this.arrayPath]);
    }
    
    private transformDescriptor(
        descriptor: TypeDescriptor, 
        remainingPath: string[]
    ): TypeDescriptor {
        if (remainingPath.length === 0) {
            return descriptor;
        }
        
        const [currentArrayName, ...restPath] = remainingPath;
        
        if (restPath.length === 0) {
            // This is the target array - remove it from the descriptor
            return {
                arrays: descriptor.arrays.filter(a => a.name !== currentArrayName)
            };
        }
        
        // Navigate deeper
        return {
            arrays: descriptor.arrays.map(arrayDesc => {
                if (arrayDesc.name === currentArrayName) {
                    return {
                        name: arrayDesc.name,
                        type: this.transformDescriptor(arrayDesc.type, restPath)
                    };
                }
                return arrayDesc;
            })
        };
    }
    
    onAdded(pathNames: string[], handler: AddedHandler): void {
        if (this.isParentPath(pathNames)) {
            // Handler wants events at the parent level (where aggregate lives)
            this.input.onAdded(pathNames, handler);
        } else if (this.isBelowTargetArray(pathNames)) {
            // Handler wants events below the target array
            // Do nothing - the array no longer exists in output
        } else {
            // Handler wants events at unrelated path - pass through
            this.input.onAdded(pathNames, handler);
        }
    }
    
    onRemoved(pathNames: string[], handler: RemovedHandler): void {
        if (this.isParentPath(pathNames)) {
            // Handler wants events at the parent level
            this.input.onRemoved(pathNames, handler);
        } else if (this.isBelowTargetArray(pathNames)) {
            // Handler wants events below the target array
            // Do nothing - the array no longer exists in output
        } else {
            // Pass through
            this.input.onRemoved(pathNames, handler);
        }
    }
    
    onModified(pathNames: string[], handler: ModifiedHandler): void {
        if (this.isParentPath(pathNames)) {
            // Handler wants modification events at parent level
            // This is the ONLY channel for receiving aggregate values
            this.modifiedHandlers.push({
                pathNames,
                handler
            });
            
            // Also pass through to input for other property modifications
            this.input.onModified(pathNames, handler);
        } else if (this.isBelowTargetArray(pathNames)) {
            // Handler wants events below the target array
            // Do nothing - the array no longer exists in output
        } else {
            this.input.onModified(pathNames, handler);
        }
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
     * Checks if the given path is below the target array (now aggregated)
     */
    private isBelowTargetArray(pathNames: string[]): boolean {
        if (pathNames.length < this.arrayPath.length) {
            return false;
        }
        
        // Check if pathNames starts with arrayPath
        return this.arrayPath.every((name, i) => pathNames[i] === name);
    }
    
    /**
     * Handle when an item is added to the target array
     */
    private handleItemAdded(runtimePath: string[], itemKey: string, item: ImmutableProps): void {
        // runtimePath contains the hash keys leading to this item
        // For arrayPath ['cities', 'venues'], runtimePath might be ['hash_TX', 'hash_Dallas']
        // The full item path would be runtimePath + [itemKey]
        
        const parentPath = runtimePath;
        const parentHash = computePathHash(parentPath);
        const itemPath = [...runtimePath, itemKey];
        const itemHash = computePathHash(itemPath);
        
        // Store item for later removal
        this.itemStore.set(itemHash, item);
        
        // Compute new aggregate
        const currentAggregate = this.aggregateValues.get(parentHash);
        const newAggregate = this.config.add(currentAggregate, item);
        this.aggregateValues.set(parentHash, newAggregate);
        
        // Emit modification event
        // The parent key is the last element of parentPath
        // The path to the parent is everything before that
        if (parentPath.length > 0) {
            const parentKey = parentPath[parentPath.length - 1];
            const pathToParent = parentPath.slice(0, -1);
            
            this.modifiedHandlers.forEach(({ handler }) => {
                handler(pathToParent, parentKey, this.propertyName, newAggregate);
            });
        } else {
            // Parent is at root level - edge case
            // In this case, modifiedHandlers at root level would receive updates
            // But this scenario should be handled differently based on the design
            // For now, we'll emit to handlers registered at root
            this.modifiedHandlers.forEach(({ handler }) => {
                handler([], '', this.propertyName, newAggregate);
            });
        }
    }
    
    /**
     * Handle when an item is removed from the target array
     */
    private handleItemRemoved(runtimePath: string[], itemKey: string): void {
        const parentPath = runtimePath;
        const parentHash = computePathHash(parentPath);
        const itemPath = [...runtimePath, itemKey];
        const itemHash = computePathHash(itemPath);
        
        // Lookup stored item data
        const item = this.itemStore.get(itemHash);
        if (!item) {
            throw new Error(`Item ${itemKey} not found in item store`);
        }
        
        // Remove from tracking
        this.itemStore.delete(itemHash);
        
        // Get current aggregate
        const currentAggregate = this.aggregateValues.get(parentHash);
        if (currentAggregate === undefined) {
            throw new Error(`No aggregate value for parent ${parentHash}`);
        }
        
        // Compute new aggregate
        const newAggregate = this.config.subtract(currentAggregate, item);
        
        // Check if there are remaining items for this parent
        // We do this by checking if any items in itemStore have the same parent hash prefix
        let hasRemainingItems = false;
        const storedItemHashes = Array.from(this.itemStore.keys());
        for (let i = 0; i < storedItemHashes.length; i++) {
            if (storedItemHashes[i].startsWith(parentHash + '::')) {
                hasRemainingItems = true;
                break;
            }
        }
        
        if (hasRemainingItems) {
            this.aggregateValues.set(parentHash, newAggregate);
        } else {
            this.aggregateValues.delete(parentHash);
        }
        
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