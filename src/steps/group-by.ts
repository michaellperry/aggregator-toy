import type { AddedHandler, ImmutableProps, RemovedHandler, Step } from '../pipeline';
import { type TypeDescriptor } from '../pipeline';
import { computeKeyHash } from "../util/hash";
import { pathsMatch, pathStartsWith } from "../util/path";

export class GroupByStep<T extends {}, K extends keyof T, ArrayName extends string> implements Step {
    groupAddedHandlers: AddedHandler[] = [];
    itemAddedHandlers: AddedHandler[] = [];
    groupRemovedHandlers: RemovedHandler[] = [];
    itemRemovedHandlers: RemovedHandler[] = [];

    keyToGroupHash: Map<string, string> = new Map<string, string>();
    groupToKeys: Map<string, Set<string>> = new Map<string, Set<string>>();
    
    // Maps item key to its parent path for correct emission
    keyToParentPath: Map<string, string[]> = new Map<string, string[]>();

    constructor(
        private input: Step,
        private keyProperties: K[],
        private arrayName: ArrayName,
        private scopePath: string[] = []  // Path where this groupBy operates
    ) {
        // Register with the input step to receive items at the scope path level
        this.input.onAdded(this.scopePath, (path, key, immutableProps) => {
            this.handleAdded(path, key, immutableProps);
        });
        this.input.onRemoved(this.scopePath, (path, key) => {
            this.handleRemoved(path, key);
        });
    }

    getTypeDescriptor(): TypeDescriptor {
        const inputDescriptor = this.input.getTypeDescriptor();
        
        if (this.scopePath.length === 0) {
            // Root level: wrap with the new array
            return {
                arrays: [
                    {
                        name: this.arrayName,
                        type: inputDescriptor  // Items have the input type
                    }
                ]
            };
        } else {
            // Scoped level: navigate to scope and transform there
            return this.transformDescriptorAtPath(inputDescriptor, [...this.scopePath]);
        }
    }
    
    /**
     * Transforms the type descriptor at the specified path to add the groupBy result.
     */
    private transformDescriptorAtPath(descriptor: TypeDescriptor, remainingPath: string[]): TypeDescriptor {
        if (remainingPath.length === 0) {
            // We're at the target level - wrap with new array
            return {
                arrays: [
                    {
                        name: this.arrayName,
                        type: descriptor
                    }
                ]
            };
        }
        
        const [currentArrayName, ...restPath] = remainingPath;
        
        return {
            arrays: descriptor.arrays.map(arrayDesc => {
                if (arrayDesc.name === currentArrayName) {
                    return {
                        name: arrayDesc.name,
                        type: this.transformDescriptorAtPath(arrayDesc.type, restPath)
                    };
                }
                return arrayDesc;
            })
        };
    }

    onAdded(pathNames: string[], handler: AddedHandler): void {
        // Check if pathNames matches our scope + group level
        if (this.isAtGroupLevel(pathNames)) {
            // Handler is at the group level (scope path)
            this.groupAddedHandlers.push(handler);
        } else if (this.isAtItemLevel(pathNames)) {
            // Handler is at the item level (scope path + arrayName)
            this.itemAddedHandlers.push(handler);
        } else if (this.isBelowItemLevel(pathNames)) {
            // Handler is below this array in the tree
            const scopeAndArrayPath = [...this.scopePath, this.arrayName];
            const shiftedPath = pathNames.slice(scopeAndArrayPath.length);
            
            // Register interceptor with input at scope path + shifted path
            this.input.onAdded([...this.scopePath, ...shiftedPath], (notifiedPath, key, immutableProps) => {
                // notifiedPath is relative to scopePath, first element is the item hash
                const itemHash = notifiedPath[this.scopePath.length];
                const groupHash = this.keyToGroupHash.get(itemHash);
                if (groupHash === undefined) {
                    throw new Error(`GroupByStep: item with key "${itemHash}" not found when handling nested path addition notification`);
                }
                // Insert groupHash at the correct position
                const modifiedPath = [
                    ...notifiedPath.slice(0, this.scopePath.length),
                    groupHash,
                    ...notifiedPath.slice(this.scopePath.length)
                ];
                handler(modifiedPath, key, immutableProps);
            });
        } else {
            this.input.onAdded(pathNames, handler);
        }
    }

    onRemoved(pathNames: string[], handler: RemovedHandler): void {
        if (this.isAtGroupLevel(pathNames)) {
            // Handler is at the group level
            this.groupRemovedHandlers.push(handler);
        } else if (this.isAtItemLevel(pathNames)) {
            // Handler is at the item level
            this.itemRemovedHandlers.push(handler);
        } else if (this.isBelowItemLevel(pathNames)) {
            // Handler is below this array in the tree
            const scopeAndArrayPath = [...this.scopePath, this.arrayName];
            const shiftedPath = pathNames.slice(scopeAndArrayPath.length);
            
            // Register interceptor with input at scope path + shifted path
            this.input.onRemoved([...this.scopePath, ...shiftedPath], (notifiedPath, key) => {
                const itemHash = notifiedPath[this.scopePath.length];
                const groupHash = this.keyToGroupHash.get(itemHash);
                if (groupHash === undefined) {
                    throw new Error(`GroupByStep: item with key "${itemHash}" not found when handling nested path removal notification`);
                }
                const modifiedPath = [
                    ...notifiedPath.slice(0, this.scopePath.length),
                    groupHash,
                    ...notifiedPath.slice(this.scopePath.length)
                ];
                handler(modifiedPath, key);
            });
        } else {
            this.input.onRemoved(pathNames, handler);
        }
    }

    onModified(pathNames: string[], handler: (path: string[], key: string, name: string, value: any) => void): void {
        if (this.isAtGroupLevel(pathNames)) {
            // The group level is immutable
        } else if (this.isAtItemLevel(pathNames) || this.isBelowItemLevel(pathNames)) {
            // Shift the path appropriately
            const scopeAndArrayPath = [...this.scopePath, this.arrayName];
            const shiftedPath = pathNames.slice(scopeAndArrayPath.length);
            this.input.onModified([...this.scopePath, ...shiftedPath], (notifiedPath, key, name, value) => {
                const itemHash = notifiedPath[this.scopePath.length];
                const groupHash = this.keyToGroupHash.get(itemHash);
                if (groupHash === undefined) {
                    throw new Error(`GroupByStep: item with key "${itemHash}" not found when handling nested path modification notification`);
                }
                const modifiedPath = [
                    ...notifiedPath.slice(0, this.scopePath.length),
                    groupHash,
                    ...notifiedPath.slice(this.scopePath.length)
                ];
                handler(modifiedPath, key, name, value);
            });
        } else {
            this.input.onModified(pathNames, handler);
        }
    }

    /**
     * Checks if pathNames is at the group level (same as scopePath)
     */
    private isAtGroupLevel(pathNames: string[]): boolean {
        return pathsMatch(pathNames, this.scopePath);
    }
    
    /**
     * Checks if pathNames is at the item level (scopePath + arrayName)
     */
    private isAtItemLevel(pathNames: string[]): boolean {
        const itemPath = [...this.scopePath, this.arrayName];
        return pathsMatch(pathNames, itemPath);
    }
    
    /**
     * Checks if pathNames is below the item level
     */
    private isBelowItemLevel(pathNames: string[]): boolean {
        const itemPath = [...this.scopePath, this.arrayName];
        return pathNames.length > itemPath.length && pathStartsWith(pathNames, itemPath);
    }

    private handleAdded(path: string[], key: string, immutableProps: ImmutableProps) {
        // path is the runtime path at the scope level - store it for emissions
        const parentPath = path;
        this.keyToParentPath.set(key, parentPath);
        
        // Extract the key properties from the object
        let keyProps: ImmutableProps = {};
        Object.keys(immutableProps).forEach(prop => {
            if (this.keyProperties.includes(prop as K)) {
                keyProps[prop] = immutableProps[prop];
            }
        });
        // Compute the hash of the extracted properties
        const keyHash = computeKeyHash(keyProps, this.keyProperties.map(prop => prop.toString()));
        
        // Store key-to-group mapping
        this.keyToGroupHash.set(key, keyHash);
        
        // Add key to group's set
        const isNewGroup = !this.groupToKeys.has(keyHash);
        if (isNewGroup) {
            this.groupToKeys.set(keyHash, new Set<string>());
            // Notify the group handlers of the new group object at the parent path
                this.groupAddedHandlers.forEach(handler => handler(parentPath, keyHash, keyProps));
        }
        this.groupToKeys.get(keyHash)!.add(key);
        
        // Extract the non-key properties from the object
        let nonKeyProps: ImmutableProps = {};
        Object.keys(immutableProps).forEach(prop => {
            if (!this.keyProperties.includes(prop as K)) {
                nonKeyProps[prop] = immutableProps[prop];
            }
        });
        // Notify the item handlers of the new item object at parent path + keyHash
        this.itemAddedHandlers.forEach(handler => handler([...parentPath, keyHash], key, nonKeyProps));
    }

    private handleRemoved(path: string[], key: string) {
        // Get the parent path for this key
        const parentPath = this.keyToParentPath.get(key) || path;
        
        // Look up group hash
        const keyHash = this.keyToGroupHash.get(key);
        if (keyHash === undefined) {
            throw new Error(`GroupByStep: item with key "${key}" not found`);
        }
        
        // Notify item removed handlers at parent path + keyHash
        this.itemRemovedHandlers.forEach(handler => handler([...parentPath, keyHash], key));
        
        // Remove key from tracking
        this.keyToGroupHash.delete(key);
        this.keyToParentPath.delete(key);
        
        // Remove key from group's set
        const groupKeys = this.groupToKeys.get(keyHash);
        if (groupKeys) {
            groupKeys.delete(key);
            
            // Check if group is empty
            if (groupKeys.size === 0) {
                // Notify group removed handlers at parent path
                this.groupRemovedHandlers.forEach(handler => handler(parentPath, keyHash));
                
                // Clean up tracking
                this.groupToKeys.delete(keyHash);
            }
        }
    }
}

