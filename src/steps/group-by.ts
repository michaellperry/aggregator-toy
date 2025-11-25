import type { Step } from '../pipeline';
import { computeKeyHash } from '../util/hash';

export class GroupByStep<T extends {}, K extends keyof T, ArrayName extends string> implements Step<Pick<T, K> & Record<ArrayName, Omit<T, K>[]>> {
    private groups: Map<string, { groupKey: string, keyProps: Pick<T, K>, items: Map<string, Omit<T, K>> }> = new Map();
    private itemToGroup: Map<string, string> = new Map();
    private addedHandler?: (path: string[], key: string, immutableProps: Pick<T, K> & Record<ArrayName, Omit<T, K>[]>) => void;

    constructor(
        private input: Step<T>,
        private keyProperties: K[],
        private arrayName: ArrayName
    ) {}

    onAdded(handler: (path: string[], key: string, immutableProps: Pick<T, K> & Record<ArrayName, Omit<T, K>[]>) => void): void {
        this.addedHandler = handler;
        this.input.onAdded((path, itemKey, immutableProps) => {
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

                // Emit the group with empty array initially
                const groupOutput: Pick<T, K> & Record<ArrayName, Omit<T, K>[]> = {
                    ...group.keyProps,
                    [this.arrayName]: [] as Omit<T, K>[]
                } as Pick<T, K> & Record<ArrayName, Omit<T, K>[]>;
                handler([], groupKey, groupOutput);
            }

            // Add item to group
            group.items.set(itemKey, nonKeyProps as Omit<T, K>);
            this.itemToGroup.set(itemKey, groupHash);

            // Update the group's array and emit the updated group
            const updatedGroup: Pick<T, K> & Record<ArrayName, Omit<T, K>[]> = {
                ...group.keyProps,
                [this.arrayName]: Array.from(group.items.values())
            } as Pick<T, K> & Record<ArrayName, Omit<T, K>[]>;
            handler([], group.groupKey, updatedGroup);
        });
    }

    onRemoved(handler: (path: string[], key: string) => void): void {
        this.input.onRemoved((path, itemKey) => {
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

            if (group.items.size === 0) {
                // Group is empty, remove it
                this.groups.delete(groupHash);
                handler([], group.groupKey);
            } else {
                // Update the group's array and emit the updated group via onAdded handler
                // Do this BEFORE calling the removal handler to ensure the group is updated
                if (this.addedHandler) {
                    const updatedGroup: Pick<T, K> & Record<ArrayName, Omit<T, K>[]> = {
                        ...group.keyProps,
                        [this.arrayName]: Array.from(group.items.values())
                    } as Pick<T, K> & Record<ArrayName, Omit<T, K>[]>;
                    this.addedHandler([], group.groupKey, updatedGroup);
                }
                // Don't call handler here - the item removal doesn't remove the group
            }
        });
    }
}

