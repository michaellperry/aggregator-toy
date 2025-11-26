import type { ImmutableProps, OnAddedHandler, OnRemovedHandler, Step } from '../pipeline';
import { getPathNamesFromDescriptor, type TypeDescriptor } from '../pipeline';
import { computeKeyHash } from "../util/hash";

export class GroupByStep<T extends {}, K extends keyof T, ArrayName extends string> implements Step {
    groupAddedHandlers: OnAddedHandler[] = [];
    itemAddedHandlers: OnAddedHandler[] = [];

    groups: Set<string> = new Set<string>();

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

    getPathNames(): string[][] {
        return getPathNamesFromDescriptor(this.getTypeDescriptor());
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
        } else {
            this.input.onAdded(path, handler);
        }
    }

    onRemoved(path: string[], handler: OnRemovedHandler): void {
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
        if (!this.groups.has(keyHash)) {
            this.groups.add(keyHash);
            // Notify the group handlers of the new group object
            this.groupAddedHandlers.forEach(handler => handler([], keyHash, keyProps));
        }
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
        throw new Error("Method not implemented.");
    }
}

