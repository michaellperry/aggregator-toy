import { createPipeline, KeyedArray, Transform, PipelineBuilder, Pipeline } from './index';

// Type helper to extract the output type from a PipelineBuilder
type BuilderOutputType<T> = T extends PipelineBuilder<any, infer U> ? U : never;

// Helper function that uses type inference to set up a test pipeline
function createTestPipeline<TStart, T>(
    builderFactory: () => PipelineBuilder<TStart, T>
): [Pipeline<TStart>, () => T[]] {
    const builder = builderFactory();
    type OutputType = BuilderOutputType<typeof builder>;
    const [ getState, setState ] = simulateState<KeyedArray<OutputType>>([]);
    const pipeline = builder.build(setState);
    return [pipeline, () => produce(getState())];
}

describe('pipeline', () => {
    it('should build an array', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ message: string }>()
        );

        pipeline.add("item1", { message: "Hello" });
        pipeline.add("item2", { message: "Goodbye" });

        const output = getOutput();

        expect(output).toEqual([
            { message: "Hello" },
            { message: "Goodbye" }
        ]);
    });

    it('should define a property', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ a: number, b: number }>()
                .defineProperty("sum", (item) => item.a + item.b)
        );

        pipeline.add("item1", { a: 2, b: 5 });
        pipeline.add("item2", { a: 4, b: -1 });

        const output = getOutput();

        expect(output).toEqual([
            { a: 2, b: 5, sum: 7 },
            { a: 4, b: -1, sum: 3 }
        ]);
    });
});

function simulateState<T>(initialState: T): [() => T, (transform: Transform<T>) => void] {
    let state: T = initialState;
    return [
        () => state,
        (transform: Transform<T>) => state = transform(state)
    ];
}


function produce<T>(state: KeyedArray<T>) : T[] {
    return state.map(item => item.value);
}