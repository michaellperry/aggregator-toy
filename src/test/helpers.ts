import { KeyedArray, Transform, PipelineBuilder, Pipeline, TypeDescriptor } from '../index';
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
    const typeDescriptor = builder.getTypeDescriptor();
    const pipeline = builder.build(setState);
    const getOutput = (): OutputType[] => extract<OutputType>(getState(), typeDescriptor);
    return [pipeline, getOutput];
}

export function simulateState<T>(initialState: T): [() => T, (transform: Transform<T>) => void] {
    let state: T = initialState;
    return [
        () => state,
        (transform: Transform<T>) => state = transform(state)
    ];
}

export function extract<T>(state: KeyedArray<T>, typeDescriptor: TypeDescriptor): T[] {
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
    
    // Helper to check if an array contains nested groups
    const arrayContainsGroups = (arrayName: string, descriptor: TypeDescriptor): boolean => {
        const arrayDesc = descriptor.arrays.find(a => a.name === arrayName);
        return arrayDesc ? arrayDesc.type.arrays.length > 0 : false;
    };
    
    // Helper function to recursively process a group and its nested arrays
    const processGroup = (group: { key: string, value: T }, descriptor: TypeDescriptor): T => {
        const groupValue = group.value as any;
        const reconstructed = { ...groupValue };
        
        // Process all arrays defined in the descriptor for this group
        descriptor.arrays.forEach(arrayDesc => {
            const arrayName = arrayDesc.name;
            
            // Get all items for this array property
            const arrayItemEntries = items
                .filter(item => item.groupKey === group.key && item.arrayName === arrayName);
            
            // Only process arrays that have items (or are defined in descriptor)
            // Empty arrays will be set to empty array below
            
            // Check if this array contains nested groups
            const isNested = arrayContainsGroups(arrayName, descriptor);
            
            const arrayItems = arrayItemEntries.map(entry => {
                if (isNested) {
                    // This array contains groups - find the nested group and process it
                    const nestedGroup = groups.find(g => g.key === entry.itemKey);
                    if (nestedGroup) {
                        // Recursively process with the nested type descriptor
                        return processGroup(nestedGroup, arrayDesc.type);
                    }
                    // If isNested is true but we don't find a group, something is wrong
                    // but return the value as fallback
                    return entry.value;
                }
                // Simple item (no nested arrays)
                return entry.value;
            });
            
            reconstructed[arrayName] = arrayItems;
        });
        
        return reconstructed as T;
    };
    
    // Find which groups are referenced as items in arrays (nested groups)
    const nestedGroupKeys = new Set(items.map(item => item.itemKey));
    
    // Only process top-level groups (groups that are not referenced as items in arrays)
    return groups
        .filter(group => !nestedGroupKeys.has(group.key))
        .map(group => processGroup(group, typeDescriptor));
}

