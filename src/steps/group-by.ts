import type { Step } from '../pipeline';
import { computeKeyHash } from '../util/hash';
import { createCompositeKey, parseCompositeKey } from '../util/composite-key';
import { KeyedArray } from "../builder";

export class GroupByStep<T extends {}, K extends keyof T, ArrayName extends string> implements Step<Pick<T, K> & Record<ArrayName, KeyedArray<Omit<T, K>>>> {
    private groups: Map<string, { groupKey: string, keyProps: Pick<T, K>, items: Map<string, Omit<T, K>> }> = new Map();
    private itemToGroup: Map<string, string> = new Map();
    private addedHandlers: Map<string, (path: string[], key: string, immutableProps: any) => void> = new Map();
    private removedHandlers: Map<string, (path: string[], key: string) => void> = new Map();

    constructor(
        private input: Step<T>,
        private keyProperties: K[],
        private arrayName: ArrayName
    ) {}

    getPaths(): string[][] {
        return [[], [this.arrayName]];
    }

    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: Pick<T, K> & Record<ArrayName, KeyedArray<Omit<T, K>>>) => void): void {
        const pathKey = path.join(':');
        this.addedHandlers.set(pathKey, handler);
        
        // Set up input handler only once
        if (this.addedHandlers.size === 1) {
            this.input.onAdded([], (inputPath, itemKey, immutableProps) => {
                // Compute group hash from key properties
                const keyProps = this.keyProperties.map(prop => String(prop));
                const groupHash = computeKeyHash(immutableProps, keyProps);

                // Extract key properties for the group
                const keyPropsObj: Partial<Pick<T, K>> = {};
                for (const prop of this.keyProperties) {
                    keyPropsObj[prop] = immutableProps[prop];
                }

                // Extract non-key properties for the item
                const nonKeyProps: any = {};
                for (const prop in immutableProps) {
                    if (!this.keyProperties.includes(prop as any)) {
                        nonKeyProps[prop] = immutableProps[prop];
                    }
                }

                // Get or create group
                let group = this.groups.get(groupHash);
                const groupKey = groupHash;
                if (!group) {
                    // Create new group
                    group = {
                        groupKey,
                        keyProps: keyPropsObj as Pick<T, K>,
                        items: new Map()
                    };
                    this.groups.set(groupHash, group);

                    // Emit the group (path [])
                    const groupHandler = this.addedHandlers.get('');
                    if (groupHandler) {
                        groupHandler([], groupKey, group.keyProps);
                    }
                }

                // Add item to group
                group.items.set(itemKey, nonKeyProps as Omit<T, K>);
                this.itemToGroup.set(itemKey, groupHash);

                // Emit the item (path [arrayName])
                // Note: For items, we emit Omit<T, K> but the handler signature expects the full type
                // The builder will handle this appropriately
                const itemHandler = this.addedHandlers.get(this.arrayName);
                if (itemHandler) {
                    const compositeKey = createCompositeKey(groupKey, this.arrayName, itemKey);
                    // Cast to satisfy type system - builder knows how to handle this
                    itemHandler([this.arrayName], compositeKey, nonKeyProps as any);
                }
            });
        }
    }

    onRemoved(path: string[], handler: (path: string[], key: string) => void): void {
        const pathKey = path.join(':');
        this.removedHandlers.set(pathKey, handler);
        
        // Set up input handler only once
        if (this.removedHandlers.size === 1) {
            this.input.onRemoved([], (inputPath, itemKey) => {
                const groupHash = this.itemToGroup.get(itemKey);
                if (!groupHash) {
                    return; // Item not in any group (shouldn't happen)
                }

                const group = this.groups.get(groupHash);
                if (!group) {
                    return; // Group doesn't exist (shouldn't happen)
                }

                // Remove item from group
                group.items.delete(itemKey);
                this.itemToGroup.delete(itemKey);

                // Emit item removal (path [arrayName])
                const itemRemovalHandler = this.removedHandlers.get(this.arrayName);
                if (itemRemovalHandler) {
                    const compositeKey = createCompositeKey(groupHash, this.arrayName, itemKey);
                    itemRemovalHandler([this.arrayName], compositeKey);
                }

                if (group.items.size === 0) {
                    // Group is empty, remove it
                    this.groups.delete(groupHash);
                    // Emit group removal (path [])
                    const groupRemovalHandler = this.removedHandlers.get('');
                    if (groupRemovalHandler) {
                        groupRemovalHandler([], group.groupKey);
                    }
                }
            });
        }
    }
}

