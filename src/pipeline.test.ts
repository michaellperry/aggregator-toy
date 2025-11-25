type KeyedArray<T> = { key: string, value: T }[];

type Transform<T> = (state: T) => T;

interface Pipeline<T> {
    add(key: string, immutableProps: T): void;
}

interface Step<T> {
    onAdded(handler: (key: string, immutableProps: T) => void): void;
}

class DefinePropertyStep<T, K extends string, U> implements Step<T & Record<K, U>> {
    constructor(private input: Step<T>, private propertyName: K, private compute: (item: T) => U) {}
    onAdded(handler: (key: string, immutableProps: T & Record<K, U>) => void): void {
        this.input.onAdded((key, immutableProps) => {
            handler(key, { ...immutableProps, [this.propertyName]: this.compute(immutableProps) } as T & Record<K, U>);
        });
    }
}

class PipelineBuilder<TStart, T> {
    constructor(private input: Pipeline<TStart>,private lastStep: Step<T>) {}

    defineProperty<K extends string, U>(propertyName: K, compute: (item: T) => U): PipelineBuilder<TStart, T & Record<K, U>> {
        const newStep = new DefinePropertyStep(this.lastStep, propertyName, compute);
        return new PipelineBuilder<TStart, T & Record<K, U>>(this.input, newStep);
    }
    build(setState: (transform: Transform<KeyedArray<T>>) => void): Pipeline<TStart> {
        this.lastStep.onAdded((key, immutableProps) => {
            setState(state => [...state, { key, value: immutableProps }]);
        });
        return this.input;
    }
}

class InputPipeline<T> implements Pipeline<T>, Step<T> {
    private handlers: ((key: string, immutableProps: T) => void)[] = [];

    add(key: string, immutableProps: T): void {
        this.handlers.forEach(handler => handler(key, immutableProps));
    }

    onAdded(handler: (key: string, immutableProps: T) => void): void {
        this.handlers.push(handler);
    }
}

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

function createPipeline<TStart>(): PipelineBuilder<TStart, TStart> {
    const start = new InputPipeline<TStart>();
    return new PipelineBuilder<TStart, TStart>(start, start);
}

function produce<T>(state: KeyedArray<T>) : T[] {
    return state.map(item => item.value);
}