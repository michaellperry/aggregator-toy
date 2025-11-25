interface Item {
    message: string;
}

type State = { key: string, value: Item }[];

type Transform<T> = (state: T) => T;

interface Pipeline {
    onAdded(key: string, immutableProps: Item): void;
}

describe('pipeline', () => {
    it('should build an array', () => {
        // Define a state reducer.
        let state: State = [];
        const setState = (transform: Transform<State>) => {
            state = transform(state);
        }

        // Set up a pipeline.
        const pipeline = createPipeline(setState);

        // Exercise the pipeline to inject objects.
        pipeline.onAdded("item1", { message: "Hello" });
        pipeline.onAdded("item2", { message: "Goodbye" });

        // Observe the output of the pipeline.
        const output = produce(state);

        expect(output).toEqual([
            { message: "Hello" },
            { message: "Goodbye" }
        ]);
    });
});

function createPipeline(setState: (transform: Transform<State>) => void): Pipeline {
    return {
        onAdded(key, immutableProps) {
            setState(state => [...state, { key, value: immutableProps }]);
        }
    };
}

function produce(state: State) : Item[] {
    return state.map(item => item.value);
}