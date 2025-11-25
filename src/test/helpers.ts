import { KeyedArray, Transform, PipelineBuilder, Pipeline } from '../index';
import { parseCompositeKey } from '../util/composite-key';

// Type helper to extract the output type from a PipelineBuilder
// and recursively convert KeyedArray properties to arrays
type ExtractKeyedArrays<T> = T extends KeyedArray<infer U>
    ? ExtractKeyedArrays<U>[]  // Convert KeyedArray<T> to T[]
    : T extends object
    ? {
          // For intersection types, we need to be more careful about which keys to include
          [K in keyof T]: T[K] extends KeyedArray<infer U>
              ? ExtractKeyedArrays<U>[]
              : ExtractKeyedArrays<T[K]>
      }
    : T;

export type BuilderOutputType<T> = T extends PipelineBuilder<any, infer U> 
    ? ExtractKeyedArrays<U> 
    : never;

// Helper function that uses type inference to set up a test pipeline
export function createTestPipeline<TBuilder extends PipelineBuilder<any, any>>(
    builderFactory: () => TBuilder
): [Pipeline<any>, () => BuilderOutputType<TBuilder>[]] {
    const builder = builderFactory();
    type OutputType = BuilderOutputType<TBuilder>;
    // Use the actual output type from the builder, not the input type
    const [ getState, setState ] = simulateState<KeyedArray<OutputType>>([]);
    const pipeline = builder.build(setState);
    const getOutput = (): OutputType[] => produce<OutputType>(getState());
    return [pipeline, getOutput];
}

export function simulateState<T>(initialState: T): [() => T, (transform: Transform<T>) => void] {
    let state: T = initialState;
    return [
        () => state,
        (transform: Transform<T>) => state = transform(state)
    ];
}

export function produce<T>(state: KeyedArray<T>) : T[] {
    // Separate groups (simple keys) from items (composite keys)
    const groups: KeyedArray<T> = [];
    const items: Array<{ groupKey: string, arrayName: string, itemKey: string, value: T }> = [];
    
    state.forEach(item => {
        const parsed = parseCompositeKey(item.key);
        if (parsed) {
            // This is a composite key (item)
            items.push({
                groupKey: parsed.groupKey,
                arrayName: parsed.arrayName,
                itemKey: parsed.itemKey,
                value: item.value
            });
        } else {
            // This is a simple key (group)
            groups.push(item);
        }
    });
    
    // Find which groups are referenced as items in arrays (nested groups)
    const nestedGroupKeys = new Set(items.map(item => item.itemKey));
    
    // Helper function to recursively process a group and its nested arrays
    const processGroup = (group: { key: string, value: T }): T => {
        const groupValue = group.value as any;
        const reconstructed = { ...groupValue };
        
        // Find all array properties for this group
        const arrayNames = new Set(items
            .filter(item => item.groupKey === group.key)
            .map(item => item.arrayName));
        
        arrayNames.forEach(arrayName => {
            // Get all items for this array property
            const arrayItemEntries = items
                .filter(item => item.groupKey === group.key && item.arrayName === arrayName);
            
            // For each item, check if it's a nested group that needs recursive processing
            const arrayItems = arrayItemEntries.map(entry => {
                // Check if this item key corresponds to a group (nested structure)
                const nestedGroup = groups.find(g => g.key === entry.itemKey);
                if (nestedGroup) {
                    // This is a nested group - recursively process it
                    return processGroup(nestedGroup);
                } else {
                    // This is a simple item
                    return entry.value;
                }
            });
            
            reconstructed[arrayName] = arrayItems;
        });
        
        return reconstructed as T;
    };
    
    // Only process top-level groups (groups that are not referenced as items in arrays)
    return groups
        .filter(group => !nestedGroupKeys.has(group.key))
        .map(group => processGroup(group));
}

