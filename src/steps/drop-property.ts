import type { AddedHandler, ModifiedHandler, RemovedHandler, Step } from '../pipeline';
import { type TypeDescriptor } from '../pipeline';
import { pathsMatch } from '../util/path';

export class DropPropertyStep<T, K extends keyof T> implements Step {
    private isArrayProperty: boolean;
    private fullArrayPath: string[];
    
    constructor(
        private input: Step,
        private propertyName: K,
        private scopePath: string[]
    ) {
        // Check if the property is an array in the type descriptor
        const descriptor = this.input.getTypeDescriptor();
        this.fullArrayPath = [...this.scopePath, this.propertyName as string];
        this.isArrayProperty = this.isArrayInDescriptor(descriptor, this.scopePath, this.propertyName as string);
    }
    
    /**
     * Checks if a property name exists as an array in the type descriptor at the given path.
     */
    private isArrayInDescriptor(
        descriptor: TypeDescriptor,
        path: string[],
        propertyName: string
    ): boolean {
        // Navigate to the scope path
        const targetDescriptor = this.navigateToPath(descriptor, path);
        
        // Check if propertyName exists in the arrays at this level
        return targetDescriptor.arrays.some(array => array.name === propertyName);
    }
    
    /**
     * Navigates through the type descriptor to reach the target path.
     */
    private navigateToPath(descriptor: TypeDescriptor, path: string[]): TypeDescriptor {
        if (path.length === 0) {
            return descriptor;
        }
        
        const [currentArrayName, ...restPath] = path;
        const arrayDesc = descriptor.arrays.find(a => a.name === currentArrayName);
        
        if (!arrayDesc) {
            // Path doesn't exist - return empty descriptor
            return { arrays: [] };
        }
        
        return this.navigateToPath(arrayDesc.type, restPath);
    }
    
    getTypeDescriptor(): TypeDescriptor {
        if (this.isArrayProperty) {
            // Remove the array from the descriptor
            const inputDescriptor = this.input.getTypeDescriptor();
            return this.transformDescriptor(inputDescriptor, [...this.fullArrayPath]);
        }
        // For non-array properties, don't modify the descriptor
        return this.input.getTypeDescriptor();
    }
    
    /**
     * Recursively transforms the type descriptor to remove the target array.
     * (Same logic as DropArrayStep)
     */
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
    
    onAdded(pathNames: string[], handler: AddedHandler): void {
        if (this.isArrayProperty) {
            // Array behavior: suppress events at or below the array path
            if (this.isAtOrBelowTargetArray(pathNames)) {
                return;
            }
            this.input.onAdded(pathNames, handler);
        } else {
            // Property behavior: filter the property from immutableProps
            if (this.isAtScopePath(pathNames)) {
                this.input.onAdded(pathNames, (path, key, immutableProps) => {
                    const { [this.propertyName]: _, ...rest } = immutableProps;
                    handler(path, key, rest as Omit<T, K>);
                });
            } else {
                this.input.onAdded(pathNames, handler);
            }
        }
    }
    
    onRemoved(pathNames: string[], handler: RemovedHandler): void {
        if (this.isArrayProperty) {
            // Array behavior: suppress events at or below the array path
            if (this.isAtOrBelowTargetArray(pathNames)) {
                return;
            }
        }
        this.input.onRemoved(pathNames, handler);
    }

    onModified(pathNames: string[], handler: ModifiedHandler): void {
        if (this.isArrayProperty) {
            // Array behavior: suppress events at or below the array path
            if (this.isAtOrBelowTargetArray(pathNames)) {
                return;
            }
        }
        this.input.onModified(pathNames, handler);
    }
    
    private isAtScopePath(pathNames: string[]): boolean {
        return pathsMatch(pathNames, this.scopePath);
    }
    
    /**
     * Checks if a path is at or below the target array.
     */
    private isAtOrBelowTargetArray(pathNames: string[]): boolean {
        if (pathNames.length < this.fullArrayPath.length) {
            return false;
        }
        return this.fullArrayPath.every((name, i) => pathNames[i] === name);
    }
}

