import type { AddedHandler, ModifiedHandler, RemovedHandler, Step, TypeDescriptor } from '../pipeline';

/**
 * A step that removes an array from the output type descriptor
 * and suppresses all events at or below that array path.
 * 
 * This step is stateless - it purely filters/suppresses events without
 * maintaining any internal state.
 *
 * @template TInput - The input type containing the array to drop
 * @template TPath - The tuple of array names forming the path to the target array
 */
export class DropArrayStep<
    TInput,
    TPath extends string[]
> implements Step {
    
    constructor(
        private input: Step,
        private arrayPath: TPath
    ) {
        // No event handlers to register - this step only filters
    }
    
    getTypeDescriptor(): TypeDescriptor {
        const inputDescriptor = this.input.getTypeDescriptor();
        return this.transformDescriptor(inputDescriptor, [...this.arrayPath]);
    }
    
    /**
     * Recursively transforms the type descriptor to remove the target array.
     * 
     * @param descriptor - The current type descriptor
     * @param remainingPath - The remaining path segments to navigate
     * @returns The transformed descriptor with the target array removed
     */
    private transformDescriptor(
        descriptor: TypeDescriptor, 
        remainingPath: string[]
    ): TypeDescriptor {
        if (remainingPath.length === 0) {
            // No more path to navigate - return unchanged
            return descriptor;
        }
        
        const [currentArrayName, ...restPath] = remainingPath;
        
        if (restPath.length === 0) {
            // This is the target array - remove it from the descriptor
            return {
                arrays: descriptor.arrays.filter(a => a.name !== currentArrayName)
            };
        }
        
        // Navigate deeper into the tree
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
    
    /**
     * Registers an added handler if the path is NOT at or below the target array.
     * Paths at or below the target array are silently ignored (suppressed).
     */
    onAdded(pathNames: string[], handler: AddedHandler): void {
        if (this.isAtOrBelowTargetArray(pathNames)) {
            // Path is at or below the dropped array
            // Silently suppress - do not register handler
            return;
        }
        
        // Path is above or unrelated to the dropped array
        // Pass through to input step
        this.input.onAdded(pathNames, handler);
    }
    
    /**
     * Registers a removed handler if the path is NOT at or below the target array.
     * Paths at or below the target array are silently ignored (suppressed).
     */
    onRemoved(pathNames: string[], handler: RemovedHandler): void {
        if (this.isAtOrBelowTargetArray(pathNames)) {
            // Suppress events at or below the dropped array
            return;
        }
        
        this.input.onRemoved(pathNames, handler);
    }
    
    /**
     * Registers a modified handler if the path is NOT at or below the target array.
     * Paths at or below the target array are silently ignored (suppressed).
     */
    onModified(pathNames: string[], handler: ModifiedHandler): void {
        if (this.isAtOrBelowTargetArray(pathNames)) {
            // Suppress events at or below the dropped array
            return;
        }
        
        this.input.onModified(pathNames, handler);
    }
    
    /**
     * Checks if a path is at or below the target array.
     * 
     * @param pathNames - The path being checked (e.g., ['cities', 'venues'])
     * @returns true if the path starts with the arrayPath (at or below target)
     * 
     * @example
     * // arrayPath = ['cities', 'venues']
     * isAtOrBelowTargetArray(['cities']) -> false         // above target
     * isAtOrBelowTargetArray(['cities', 'venues']) -> true  // at target
     * isAtOrBelowTargetArray(['cities', 'venues', 'staff']) -> true  // below target
     * isAtOrBelowTargetArray(['stores']) -> false         // unrelated
     */
    private isAtOrBelowTargetArray(pathNames: string[]): boolean {
        // Path must be at least as long as arrayPath to be at or below
        if (pathNames.length < this.arrayPath.length) {
            return false;
        }
        
        // Check if pathNames starts with arrayPath
        return this.arrayPath.every((name, i) => pathNames[i] === name);
    }
}