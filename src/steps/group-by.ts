import type { AddedHandler, ImmutableProps, RemovedHandler, Step } from '../pipeline';
import { type TypeDescriptor } from '../pipeline';
import { computeKeyHash } from "../util/hash";

export class GroupByStep<T extends {}, K extends keyof T, ArrayName extends string> implements Step {
    groupAddedHandlers: AddedHandler[] = [];
    itemAddedHandlers: AddedHandler[] = [];
    groupRemovedHandlers: RemovedHandler[] = [];
    itemRemovedHandlers: RemovedHandler[] = [];

    keyToGroupHash: Map<string, string> = new Map<string, string>();
    groupToKeys: Map<string, Set<string>> = new Map<string, Set<string>>();

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

    onAdded(path: string[], handler: AddedHandler): void {
        if (path.length === 0) {
            // Handler is at the group level
            this.groupAddedHandlers.push(handler);
        } else if (path.length === 1 && path[0] === this.arrayName) {
            // Handler is at the item level
            this.itemAddedHandlers.push(handler);
        } else if (path.length > 1 && path[0] === this.arrayName) {
            // Handler is below this array in the tree
            const shiftedPath = path.slice(1);
            
            // Register interceptor with input
            this.input.onAdded(shiftedPath, (notifiedPath, key, immutableProps) => {
                const itemHash = notifiedPath[0];
                const groupHash = this.keyToGroupHash.get(itemHash);
                if (groupHash === undefined) {
                    throw new Error(`GroupByStep: item with key "${itemHash}" not found when handling nested path addition notification`);
                }
                const modifiedPath = [groupHash, ...notifiedPath];
                handler(modifiedPath, key, immutableProps);
            });
        } else {
            this.input.onAdded(path, handler);
        }
    }

    onRemoved(path: string[], handler: RemovedHandler): void {
        if (path.length === 0) {
            // Handler is at the group level
            this.groupRemovedHandlers.push(handler);
        } else if (path.length === 1 && path[0] === this.arrayName) {
            // Handler is at the item level
            this.itemRemovedHandlers.push(handler);
        } else if (path.length > 1 && path[0] === this.arrayName) {
            // Handler is below this array in the tree
            const shiftedPath = path.slice(1);
            
            // Register interceptor with input
            this.input.onRemoved(shiftedPath, (notifiedPath, key) => {
                const itemHash = notifiedPath[0];
                const groupHash = this.keyToGroupHash.get(itemHash);
                if (groupHash === undefined) {
                    throw new Error(`GroupByStep: item with key "${itemHash}" not found when handling nested path removal notification`);
                }
                const modifiedPath = [groupHash, ...notifiedPath];
                handler(modifiedPath, key);
            });
        } else {
            this.input.onRemoved(path, handler);
        }
    }

    onModified(path: string[], handler: (path: string[], key: string, name: string, value: any) => void): void {
        if (path.length === 0) {
            // The group level is immutable
        } else if (path[0] === this.arrayName) {
            // Shift the path by one
            const shiftedPath = path.slice(1);
            this.input.onModified(shiftedPath, (notifiedPath, key, name, value) => {
                const itemHash = notifiedPath[0];
                const groupHash = this.keyToGroupHash.get(itemHash);
                if (groupHash === undefined) {
                    throw new Error(`GroupByStep: item with key "${itemHash}" not found when handling nested path modification notification`);
                }
                const modifiedPath = [groupHash, ...notifiedPath];
                handler(modifiedPath, key, name, value);
            });
        } else {
            this.input.onModified(path, handler);
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
            }
        }
    }
}

