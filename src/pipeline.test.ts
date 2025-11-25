interface Item {
    message: string;
}

type KeyedArray<T> = { key: string, value: T }[];

type Transform<T> = (state: T) => T;

interface Pipeline<T> {
    onAdded(key: string, immutableProps: T): void;
}

describe('pipeline', () => {
    it('should build an array', () => {
        // Define a state reducer.
        const [ getState, setState ] = simulateState<KeyedArray<Item>>([]);

        // Set up a pipeline.
        const pipeline = createPipeline(setState);

        // Exercise the pipeline to inject objects.
        pipeline.onAdded("item1", { message: "Hello" });
        pipeline.onAdded("item2", { message: "Goodbye" });

        // Observe the output of the pipeline.
        const output = produce(getState());

        expect(output).toEqual([
            { message: "Hello" },
            { message: "Goodbye" }
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

function createPipeline<T>(setState: (transform: Transform<KeyedArray<T>>) => void): Pipeline<T> {
    return {
        onAdded(key, immutableProps) {
            setState(state => [...state, { key, value: immutableProps }]);
        }
    };
}

function produce<T>(state: KeyedArray<T>) : T[] {
    return state.map(item => item.value);
}