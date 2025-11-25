import { createPipeline, KeyedArray, Transform, PipelineBuilder, Pipeline } from './index';

// Type helper to extract the output type from a PipelineBuilder
type BuilderOutputType<T> = T extends PipelineBuilder<any, infer U> ? U : never;

// Helper function that uses type inference to set up a test pipeline
function createTestPipeline<TStart, T extends {}>(
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

    it('should drop a property', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ a: number, b: number, c: string }>()
                .dropProperty("c")
        );

        pipeline.add("item1", { a: 2, b: 5, c: "test" });
        pipeline.add("item2", { a: 4, b: -1, c: "foo" });

        const output = getOutput();

        expect(output).toEqual([
            { a: 2, b: 5 },
            { a: 4, b: -1 }
        ]);
    });

    it('should remove an item after dropping a property', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ a: number, b: number, c: string }>()
                .dropProperty("c")
        );

        pipeline.add("item1", { a: 2, b: 5, c: "test" });
        pipeline.add("item2", { a: 4, b: -1, c: "foo" });
        pipeline.add("item3", { a: 10, b: 20, c: "bar" });

        expect(getOutput()).toEqual([
            { a: 2, b: 5 },
            { a: 4, b: -1 },
            { a: 10, b: 20 }
        ]);

        pipeline.remove("item2");

        expect(getOutput()).toEqual([
            { a: 2, b: 5 },
            { a: 10, b: 20 }
        ]);
    });

    it('should remove an item', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ message: string }>()
        );

        pipeline.add("item1", { message: "Hello" });
        pipeline.add("item2", { message: "Goodbye" });
        pipeline.add("item3", { message: "See you" });

        expect(getOutput()).toEqual([
            { message: "Hello" },
            { message: "Goodbye" },
            { message: "See you" }
        ]);

        pipeline.remove("item2");

        expect(getOutput()).toEqual([
            { message: "Hello" },
            { message: "See you" }
        ]);
    });

    it('should remove an item with computed properties', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ a: number, b: number }>()
                .defineProperty("sum", (item) => item.a + item.b)
        );

        pipeline.add("item1", { a: 2, b: 5 });
        pipeline.add("item2", { a: 4, b: -1 });
        pipeline.add("item3", { a: 10, b: 20 });

        expect(getOutput()).toEqual([
            { a: 2, b: 5, sum: 7 },
            { a: 4, b: -1, sum: 3 },
            { a: 10, b: 20, sum: 30 }
        ]);

        pipeline.remove("item2");

        expect(getOutput()).toEqual([
            { a: 2, b: 5, sum: 7 },
            { a: 10, b: 20, sum: 30 }
        ]);
    });

    describe('groupBy', () => {
        it('should group by single key property', () => {
            const [pipeline, getOutput] = createTestPipeline(() => 
                createPipeline<{ category: string, value: number }>()
                    .groupBy(['category'], 'items')
            );

            pipeline.add("item1", { category: 'A', value: 10 });
            pipeline.add("item2", { category: 'B', value: 20 });
            pipeline.add("item3", { category: 'A', value: 30 });

            const output = getOutput();
            expect(output.length).toBe(2);
            
            const groupA = output.find(g => g.category === 'A');
            const groupB = output.find(g => g.category === 'B');
            
            expect(groupA).toBeDefined();
            expect(groupA?.items).toEqual([{ value: 10 }, { value: 30 }]);
            
            expect(groupB).toBeDefined();
            expect(groupB?.items).toEqual([{ value: 20 }]);
        });

        it('should group by multiple key properties', () => {
            const [pipeline, getOutput] = createTestPipeline(() => 
                createPipeline<{ category: string, status: string, value: number }>()
                    .groupBy(['category', 'status'], 'items')
            );

            pipeline.add("item1", { category: 'A', status: 'active', value: 10 });
            pipeline.add("item2", { category: 'A', status: 'inactive', value: 20 });
            pipeline.add("item3", { category: 'A', status: 'active', value: 30 });

            const output = getOutput();
            expect(output.length).toBe(2);
            
            const activeGroup = output.find(g => g.category === 'A' && g.status === 'active');
            const inactiveGroup = output.find(g => g.category === 'A' && g.status === 'inactive');
            
            expect(activeGroup).toBeDefined();
            expect(activeGroup?.items).toEqual([{ value: 10 }, { value: 30 }]);
            
            expect(inactiveGroup).toBeDefined();
            expect(inactiveGroup?.items).toEqual([{ value: 20 }]);
        });

        it('should handle multiple items in same group', () => {
            const [pipeline, getOutput] = createTestPipeline(() => 
                createPipeline<{ category: string, value: number }>()
                    .groupBy(['category'], 'items')
            );

            pipeline.add("item1", { category: 'A', value: 10 });
            pipeline.add("item2", { category: 'A', value: 20 });
            pipeline.add("item3", { category: 'A', value: 30 });

            const output = getOutput();
            expect(output.length).toBe(1);
            expect(output[0].category).toBe('A');
            expect(output[0].items).toEqual([
                { value: 10 },
                { value: 20 },
                { value: 30 }
            ]);
        });

        it('should handle items in different groups', () => {
            const [pipeline, getOutput] = createTestPipeline(() => 
                createPipeline<{ category: string, value: number }>()
                    .groupBy(['category'], 'items')
            );

            pipeline.add("item1", { category: 'A', value: 10 });
            pipeline.add("item2", { category: 'B', value: 20 });
            pipeline.add("item3", { category: 'C', value: 30 });

            const output = getOutput();
            expect(output.length).toBe(3);
            
            expect(output.some(g => g.category === 'A' && g.items.length === 1)).toBe(true);
            expect(output.some(g => g.category === 'B' && g.items.length === 1)).toBe(true);
            expect(output.some(g => g.category === 'C' && g.items.length === 1)).toBe(true);
        });

        it('should remove items from groups', () => {
            const [pipeline, getOutput] = createTestPipeline(() => 
                createPipeline<{ category: string, value: number }>()
                    .groupBy(['category'], 'items')
            );

            pipeline.add("item1", { category: 'A', value: 10 });
            pipeline.add("item2", { category: 'A', value: 20 });
            pipeline.add("item3", { category: 'A', value: 30 });

            expect(getOutput()[0].items.length).toBe(3);

            pipeline.remove("item2");

            const output = getOutput();
            expect(output.length).toBe(1);
            expect(output[0].items).toEqual([
                { value: 10 },
                { value: 30 }
            ]);
        });

        it('should remove group when last item is removed', () => {
            const [pipeline, getOutput] = createTestPipeline(() => 
                createPipeline<{ category: string, value: number }>()
                    .groupBy(['category'], 'items')
            );

            pipeline.add("item1", { category: 'A', value: 10 });
            pipeline.add("item2", { category: 'B', value: 20 });

            expect(getOutput().length).toBe(2);

            pipeline.remove("item1");

            const output = getOutput();
            expect(output.length).toBe(1);
            expect(output[0].category).toBe('B');
        });

        it('should work with computed properties before groupBy', () => {
            const [pipeline, getOutput] = createTestPipeline(() => 
                createPipeline<{ category: string, a: number, b: number }>()
                    .defineProperty("sum", (item) => item.a + item.b)
                    .groupBy(['category'], 'items')
            );

            pipeline.add("item1", { category: 'A', a: 2, b: 5 });
            pipeline.add("item2", { category: 'A', a: 4, b: 1 });

            const output = getOutput();
            expect(output.length).toBe(1);
            expect(output[0].category).toBe('A');
            expect(output[0].items).toEqual([
                { a: 2, b: 5, sum: 7 },
                { a: 4, b: 1, sum: 5 }
            ]);
        });

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