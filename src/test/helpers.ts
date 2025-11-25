import { KeyedArray, Transform, PipelineBuilder, Pipeline } from '../index';
import { parseCompositeKey } from '../util/composite-key';

// Type helper to extract the output type from a PipelineBuilder
export type BuilderOutputType<T> = T extends PipelineBuilder<any, infer U> ? U : never;

// Helper function that uses type inference to set up a test pipeline
export function createTestPipeline<TStart, T extends {}>(
    builderFactory: () => PipelineBuilder<TStart, T>
): [Pipeline<TStart>, () => T[]] {
    const builder = builderFactory();
    type OutputType = BuilderOutputType<typeof builder>;
    const [ getState, setState ] = simulateState<KeyedArray<OutputType>>([]);
    const pipeline = builder.build(setState);
    return [pipeline, () => produce(getState())];
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
    
    // Reconstruct groups with their items
    return groups.map(group => {
        const groupValue = group.value as any;
        // Find all items for this group
        const groupItems = items
            .filter(item => item.groupKey === group.key)
            .map(item => item.value);
        
        // If the group value has array properties, reconstruct them
        const reconstructed = { ...groupValue };
        const arrayNames = new Set(items
            .filter(item => item.groupKey === group.key)
            .map(item => item.arrayName));
        
        arrayNames.forEach(arrayName => {
            reconstructed[arrayName] = items
                .filter(item => item.groupKey === group.key && item.arrayName === arrayName)
                .map(item => item.value);
        });
        
        return reconstructed as T;
    });
}

