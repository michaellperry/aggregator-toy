import type { ImmutableProps, OnAddedHandler, OnRemovedHandler, Step } from '../pipeline';
import { type TypeDescriptor } from '../pipeline';
import { computeKeyHash } from "../util/hash";

export class GroupByStep<T extends {}, K extends keyof T, ArrayName extends string> implements Step {
    groupAddedHandlers: OnAddedHandler[] = [];
    itemAddedHandlers: OnAddedHandler[] = [];
    groupRemovedHandlers: OnRemovedHandler[] = [];
    itemRemovedHandlers: OnRemovedHandler[] = [];
    nestedAddedHandlers: Map<string, OnAddedHandler[]> = new Map<string, OnAddedHandler[]>();
    nestedRemovedHandlers: Map<string, OnRemovedHandler[]> = new Map<string, OnRemovedHandler[]>();

    keyToGroupHash: Map<string, string> = new Map<string, string>();
    groupToKeys: Map<string, Set<string>> = new Map<string, Set<string>>();
    groupToKeyProps: Map<string, ImmutableProps> = new Map<string, ImmutableProps>();

    constructor(
        private input: Step,
        private keyProperties: K[],
        private arrayName: ArrayName
    ) {
        // Register with the input step to receive items at the root path
        this.input.onAdded([], (path, key, immutableProps) => {
            this.handleAdded(path, key, immutableProps);
        });
        this.input.onRemoved([], (path, key) => {
            this.handleRemoved(path, key);
        });
    }

    getTypeDescriptor(): TypeDescriptor {
        const inputDescriptor = this.input.getTypeDescriptor();
        return {
            arrays: [
                {
                    name: this.arrayName,
                    type: inputDescriptor  // Items have the input type
                }
            ]
        };
    }

    onAdded(path: string[], handler: OnAddedHandler): void {
        if (path.length === 0) {
            // Handler is at the group level
            this.groupAddedHandlers.push(handler);
        } else if (path.length === 1 && path[0] === this.arrayName) {
            // Handler is at the item level
            this.itemAddedHandlers.push(handler);
        } else if (path.length > 1 && path[0] === this.arrayName) {
            // Handler is below this array in the tree
            const shiftedPath = path.slice(1);
            const pathKey = shiftedPath.join(':');
            
            // Store the handler
            const handlers = this.nestedAddedHandlers.get(pathKey) || [];
            handlers.push(handler);
            this.nestedAddedHandlers.set(pathKey, handlers);
            
            // Register interceptor with input if this is the first handler for this path
            if (handlers.length === 1) {
                this.input.onAdded(shiftedPath, (notifiedPath, key, immutableProps) => {
                    const groupHash = this.keyToGroupHash.get(key);
                    if (groupHash === undefined) {
                        throw new Error(`GroupByStep: item with key "${key}" not found when handling nested path notification`);
                    }
                    const modifiedPath = [groupHash, ...notifiedPath];
                    const handlersForPath = this.nestedAddedHandlers.get(pathKey) || [];
                    handlersForPath.forEach(h => h(modifiedPath, key, immutableProps));
                });
            }
        } else {
            this.input.onAdded(path, handler);
        }
    }

    onRemoved(path: string[], handler: OnRemovedHandler): void {
        if (path.length === 0) {
            // Handler is at the group level
            this.groupRemovedHandlers.push(handler);
        } else if (path.length === 1 && path[0] === this.arrayName) {
            // Handler is at the item level
            this.itemRemovedHandlers.push(handler);
        } else if (path.length > 1 && path[0] === this.arrayName) {
            // Handler is below this array in the tree
            const shiftedPath = path.slice(1);
            const pathKey = shiftedPath.join(':');
            
            // Store the handler
            const handlers = this.nestedRemovedHandlers.get(pathKey) || [];
            handlers.push(handler);
            this.nestedRemovedHandlers.set(pathKey, handlers);
            
            // Register interceptor with input if this is the first handler for this path
            if (handlers.length === 1) {
                this.input.onRemoved(shiftedPath, (notifiedPath, key) => {
                    const groupHash = this.keyToGroupHash.get(key);
                    if (groupHash === undefined) {
                        throw new Error(`GroupByStep: item with key "${key}" not found when handling nested path removal notification`);
                    }
                    const modifiedPath = [groupHash, ...notifiedPath];
                    const handlersForPath = this.nestedRemovedHandlers.get(pathKey) || [];
                    handlersForPath.forEach(h => h(modifiedPath, key));
                });
            }
        } else {
            this.input.onRemoved(path, handler);
        }
    }

    private handleAdded(path: string[], key: string, immutableProps: ImmutableProps) {
        if (path.length !== 0) {
            throw new Error("GroupByStep notified of item added at a different level");
        }
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
            // Notify the group handlers of the new group object
            this.groupAddedHandlers.forEach(handler => handler([], keyHash, keyProps));
        }
        this.groupToKeys.get(keyHash)!.add(key);
        
        // Store key properties for the group
        this.groupToKeyProps.set(keyHash, keyProps);
        // Extract the non-key properties from the object
        let nonKeyProps: ImmutableProps = {};
        Object.keys(immutableProps).forEach(prop => {
            if (!this.keyProperties.includes(prop as K)) {
                nonKeyProps[prop] = immutableProps[prop];
            }
        });
        // Notify the item handlers of the new item object
        this.itemAddedHandlers.forEach(handler => handler([keyHash], key, nonKeyProps));
    }

    private handleRemoved(path: string[], key: string) {
        if (path.length !== 0) {
            throw new Error("GroupByStep notified of item removed at a different level");
        }
        
        // Look up group hash
        const keyHash = this.keyToGroupHash.get(key);
        if (keyHash === undefined) {
            throw new Error(`GroupByStep: item with key "${key}" not found`);
        }
        
        // Notify item removed handlers
        this.itemRemovedHandlers.forEach(handler => handler([keyHash], key));
        
        // Remove key from tracking
        this.keyToGroupHash.delete(key);
        
        // Remove key from group's set
        const groupKeys = this.groupToKeys.get(keyHash);
        if (groupKeys) {
            groupKeys.delete(key);
            
            // Check if group is empty
            if (groupKeys.size === 0) {
                // Notify group removed handlers
                this.groupRemovedHandlers.forEach(handler => handler([], keyHash));
                
                // Clean up tracking
                this.groupToKeys.delete(keyHash);
                this.groupToKeyProps.delete(keyHash);
            }
        }
    }
}

