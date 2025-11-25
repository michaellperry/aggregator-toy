import { createPipeline, KeyedArray, Transform } from './index';

describe('pipeline', () => {
    it('should build an array', () => {
        // Define a state reducer.
        const [ getState, setState ] = simulateState<KeyedArray<{ message: string }>>([]);

        // Set up a pipeline.
        const pipeline = createPipeline<{ message: string }>()
            .build(setState);

        // Exercise the pipeline to inject objects.
        pipeline.add("item1", { message: "Hello" });
        pipeline.add("item2", { message: "Goodbye" });

        // Observe the output of the pipeline.
        const output = produce(getState());

        expect(output).toEqual([
            { message: "Hello" },
            { message: "Goodbye" }
        ]);
    });

    it('should define a property', () => {
        const [ getState, setState ] = simulateState<KeyedArray<{ a: number, b: number, sum: number }>>([]);

        const pipeline = createPipeline<{ a: number, b: number }>()
            .defineProperty("sum", (item) => item.a + item.b)
            .build(setState);

        pipeline.add("item1", { a: 2, b: 5 });
        pipeline.add("item2", { a: 4, b: -1 });

        const output = produce(getState());

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