import type { AddedHandler, ImmutableProps, ModifiedHandler, RemovedHandler, Step, TypeDescriptor } from '../pipeline';

/**
 * Computes a hash key for a path (for map lookups).
 */
function computePathHash(path: string[]): string {
    return path.join('::');
}

/**
 * Tracks sum and count separately for computing average incrementally.
 */
interface AverageState {
    sum: number;
    count: number;
}

/**
 * A step that computes the average of a numeric property over items in a nested array.
 * 
 * - Returns undefined for empty arrays
 * - Tracks sum and count separately for incremental updates
 * - Handles null/undefined by excluding from both sum and count
 */
export class AverageAggregateStep<
    TInput,
    TPath extends string[],
    TPropertyName extends string
> implements Step {
    
    /** Maps parent path hash to average state (sum and count) */
    private averageStates: Map<string, AverageState> = new Map();
    
    /** Handlers for modified events at various levels */
    private modifiedHandlers: Array<{
        pathNames: string[];
        handler: ModifiedHandler;
    }> = [];
    
    constructor(
        private input: Step,
        private arrayPath: TPath,
        private propertyName: TPropertyName,
        private numericProperty: string
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
                // Update sum and count
                const state = this.averageStates.get(parentHash) || { sum: 0, count: 0 };
                state.sum += numValue;
                state.count += 1;
                this.averageStates.set(parentHash, state);
            }
        }
        
        // Compute new average
        const state = this.averageStates.get(parentHash);
        const newAverage = (state && state.count > 0) ? state.sum / state.count : undefined;
        
        // Emit modification event
        if (parentPath.length > 0) {
            const parentKey = parentPath[parentPath.length - 1];
            const pathToParent = parentPath.slice(0, -1);
            
            this.modifiedHandlers.forEach(({ handler }) => {
                handler(pathToParent, parentKey, this.propertyName, newAverage);
            });
        } else {
            // Parent is at root level
            this.modifiedHandlers.forEach(({ handler }) => {
                handler([], '', this.propertyName, newAverage);
            });
        }
    }
    
    /**
     * Handle when an item is removed from the target array
     */
    private handleItemRemoved(runtimePath: string[], itemKey: string, item: ImmutableProps): void {
        const parentPath = runtimePath;
        const parentHash = computePathHash(parentPath);
        
        // Update sum and count if value was numeric
        const value = item[this.numericProperty];
        if (value !== null && value !== undefined) {
            const numValue = Number(value);
            if (!isNaN(numValue)) {
                const state = this.averageStates.get(parentHash);
                if (state) {
                    state.sum -= numValue;
                    state.count -= 1;
                    
                    if (state.count === 0) {
                        this.averageStates.delete(parentHash);
                    } else {
                        this.averageStates.set(parentHash, state);
                    }
                }
            }
        }
        
        // Compute new average
        const state = this.averageStates.get(parentHash);
        const newAverage = (state && state.count > 0) ? state.sum / state.count : undefined;
        
        // Emit modification event
        if (parentPath.length > 0) {
            const parentKey = parentPath[parentPath.length - 1];
            const pathToParent = parentPath.slice(0, -1);
            
            this.modifiedHandlers.forEach(({ handler }) => {
                handler(pathToParent, parentKey, this.propertyName, newAverage);
            });
        } else {
            this.modifiedHandlers.forEach(({ handler }) => {
                handler([], '', this.propertyName, newAverage);
            });
        }
    }
}
