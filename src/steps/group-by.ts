import type { Step } from '../pipeline';
import { computeKeyHash } from '../util/hash';
import { createCompositeKey, parseCompositeKey } from '../util/composite-key';
import { KeyedArray } from "../builder";
import { getPathsFromDescriptor, type TypeDescriptor } from '../pipeline';

export class GroupByStep<T extends {}, K extends keyof T, ArrayName extends string> implements Step<Pick<T, K> & Record<ArrayName, KeyedArray<Omit<T, K>>>> {
    private groups: Map<string, { groupKey: string, keyProps: Pick<T, K>, items: Map<string, Omit<T, K>> }> = new Map();
    private itemToGroup: Map<string, { inputPath: string[], groupKey: string }> = new Map();
    private addedHandlers: Map<string, (path: string[], key: string, immutableProps: any) => void> = new Map();
    private removedHandlers: Map<string, (path: string[], key: string) => void> = new Map();
    private registeredInputPaths: Set<string> = new Set();

    // Track nested array names from input TypeDescriptor
    private inputArrayNames: string[] = [];

    // Buffer nested items by their parent group: arrayName -> groupKey -> items[]
    private nestedItemBuffers: Map<string, Map<string, any[]>> = new Map();

    // Track which input groups have been emitted: inputGroupKey -> outputCompositeKey
    private groupToOutputKey: Map<string, string> = new Map();

    constructor(
        private input: Step<T>,
        private keyProperties: K[],
        private arrayName: ArrayName
    ) {
        // Analyze input TypeDescriptor to find nested arrays
        const inputDescriptor = this.input.getTypeDescriptor();
        this.inputArrayNames = inputDescriptor.arrays.map(arr => arr.name);

        // Initialize buffers for each input array
        for (const arrName of this.inputArrayNames) {
            this.nestedItemBuffers.set(arrName, new Map());
        }
    }

    getTypeDescriptor(): TypeDescriptor {
        const inputDescriptor = this.input.getTypeDescriptor();
        return {
            arrays: [
                ...inputDescriptor.arrays,
                {
                    name: this.arrayName,
                    type: inputDescriptor  // Items have the input type
                }
            ]
        };
    }

    getPaths(): string[][] {
        return getPathsFromDescriptor(this.getTypeDescriptor());
    }

    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: Pick<T, K> & Record<ArrayName, KeyedArray<Omit<T, K>>>) => void): void {
        const pathKey = path.join(':');
        this.addedHandlers.set(pathKey, handler);
        
        // Set up input handlers for all input paths (only once)
        if (this.addedHandlers.size === 1) {
            const inputPaths = this.input.getPaths();
            for (const inputPath of inputPaths) {
                const inputPathKey = inputPath.join(':');
                if (this.registeredInputPaths.has(inputPathKey)) {
                    continue;
                }
                this.registeredInputPaths.add(inputPathKey);
                
                this.input.onAdded(inputPath, (inputPath, itemKey, immutableProps) => {
                    // Check if item has all required key properties
                    // Groups from path [] should always have key properties, but items from nested arrays might not
                    const props = immutableProps as Record<string, any>;
                    const hasAllKeyProperties = this.keyProperties.every(prop => {
                        const propStr = String(prop);
                        return propStr in immutableProps && props[propStr] !== undefined;
                    });
                    if (!hasAllKeyProperties) {
                        if (inputPath.length > 0 && this.inputArrayNames.includes(inputPath[0])) {
                            // Handle nested array items - buffer them
                            const inputArrayName = inputPath[0];
                            const parsed = parseCompositeKey(itemKey);
                            if (!parsed) return;
                            
                            const parentGroupKey = parsed.groupKey;
                            
                            // Buffer the item
                            const buffer = this.nestedItemBuffers.get(inputArrayName)!;
                            if (!buffer.has(parentGroupKey)) {
                                buffer.set(parentGroupKey, []);
                            }
                            buffer.get(parentGroupKey)!.push({ ...immutableProps });
                            
                            // Re-emit if parent was already emitted to output array
                            const outputKey = this.groupToOutputKey.get(parentGroupKey);
                            if (outputKey) {
                                this.reEmitWithNestedArrays(parentGroupKey, outputKey);
                            }
                            return;
                        }
                        // Original return for path [] with missing keys or non-nested array paths
                        return;
                    }

                    // Compute group hash from key properties
                    const keyProps = this.keyProperties.map(prop => String(prop));
                    const groupHash = computeKeyHash(immutableProps, keyProps);
                    
                    // Create combined key that includes input path to ensure groups from different paths are separate
                    // Use a separator that won't conflict with composite key parsing (which uses ':')
                    // For empty path, just use the hash; for non-empty paths, prefix with path
                    const combinedKey = inputPathKey ? `${inputPathKey}|${groupHash}` : groupHash;

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
                    let group = this.groups.get(combinedKey);
                    const groupKey = combinedKey;
                    if (!group) {
                        // Create new group
                        group = {
                            groupKey,
                            keyProps: keyPropsObj as Pick<T, K>,
                            items: new Map()
                        };
                        this.groups.set(combinedKey, group);

                        // Emit the group (path [])
                        const groupHandler = this.addedHandlers.get('');
                        if (groupHandler) {
                            groupHandler([], groupKey, group.keyProps);
                        }
                    }

                    // Add item to group
                    group.items.set(itemKey, nonKeyProps as Omit<T, K>);
                    this.itemToGroup.set(itemKey, { inputPath, groupKey: combinedKey });

                    // Emit the item (path [arrayName])
                    // Note: For items, we emit Omit<T, K> but the handler signature expects the full type
                    // The builder will handle this appropriately
                    const itemHandler = this.addedHandlers.get(this.arrayName);
                    if (itemHandler) {
                        const compositeKey = createCompositeKey(groupKey, this.arrayName, itemKey);
                        
                        // Include buffered nested arrays in the emitted value
                        // itemKey is the input group key when the input is from path []
                        const inputGroupKey = itemKey;
                        const completeValue = { ...nonKeyProps } as Record<string, any>;
                        for (const inputArrayName of this.inputArrayNames) {
                            const buffer = this.nestedItemBuffers.get(inputArrayName);
                            const bufferedItems = buffer?.get(inputGroupKey) || [];
                            if (bufferedItems.length > 0 || this.inputArrayNames.length > 0) {
                                completeValue[inputArrayName] = bufferedItems;
                            }
                        }
                        
                        // Cast to satisfy type system - builder knows how to handle this
                        itemHandler([this.arrayName], compositeKey, completeValue as any);
                        
                        // Track for re-emission
                        this.groupToOutputKey.set(inputGroupKey, compositeKey);
                    }
                });
            }
        }
    }

    onRemoved(path: string[], handler: (path: string[], key: string) => void): void {
        const pathKey = path.join(':');
        this.removedHandlers.set(pathKey, handler);
        
        // Set up input handlers for all input paths (only once)
        if (this.removedHandlers.size === 1) {
            const inputPaths = this.input.getPaths();
            for (const inputPath of inputPaths) {
                const inputPathKey = inputPath.join(':');
                // Register removal handler for this input path
                this.input.onRemoved(inputPath, (inputPath, itemKey) => {
                    const itemInfo = this.itemToGroup.get(itemKey);
                    if (!itemInfo) {
                        return; // Item not in any group (shouldn't happen)
                    }

                    const { groupKey } = itemInfo;
                    // groupKey is the combinedKey stored when the item was added
                    const group = this.groups.get(groupKey);
                    if (!group) {
                        return; // Group doesn't exist (shouldn't happen)
                    }

                    // Remove item from group
                    group.items.delete(itemKey);
                    this.itemToGroup.delete(itemKey);

                    // Emit item removal (path [arrayName])
                    const itemRemovalHandler = this.removedHandlers.get(this.arrayName);
                    if (itemRemovalHandler) {
                        const compositeKey = createCompositeKey(groupKey, this.arrayName, itemKey);
                        itemRemovalHandler([this.arrayName], compositeKey);
                    }

                    if (group.items.size === 0) {
                        // Group is empty, remove it
                        this.groups.delete(groupKey);
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

    private reEmitWithNestedArrays(inputGroupKey: string, outputCompositeKey: string): void {
        // Find which group this input item belongs to using itemToGroup mapping
        const itemInfo = this.itemToGroup.get(inputGroupKey);
        if (!itemInfo) return;
        
        const group = this.groups.get(itemInfo.groupKey);
        if (!group) return;
        
        // Get the non-key props for this specific item from the group's items map
        const storedNonKeyProps = group.items.get(inputGroupKey);
        if (!storedNonKeyProps) return;
        
        // Clone the nonKeyProps and attach nested arrays
        const completeValue: Record<string, any> = { ...storedNonKeyProps };
        for (const inputArrayName of this.inputArrayNames) {
            const buffer = this.nestedItemBuffers.get(inputArrayName);
            completeValue[inputArrayName] = buffer?.get(inputGroupKey) || [];
        }
        
        const itemHandler = this.addedHandlers.get(this.arrayName);
        if (itemHandler) {
            itemHandler([this.arrayName], outputCompositeKey, completeValue as any);
        }
    }
}

