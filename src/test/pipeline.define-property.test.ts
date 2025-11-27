import { createPipeline } from '../index';
import { createTestPipeline } from './helpers';

describe('pipeline defineProperty', () => {
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

    it('should only define property at root level, not at nested levels', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ category: string, value: number }>()
                .groupBy(['category'], 'items')
                .defineProperty('rootLabel' as any, (group: any) => `Root: ${group.category}`)
        );

        pipeline.add("item1", { category: 'A', value: 10 });
        pipeline.add("item2", { category: 'A', value: 20 });

        const output = getOutput() as any[];
        expect(output.length).toBe(1);
        // Root level should have the property
        expect(output[0].rootLabel).toBe('Root: A');
        // Nested items should NOT have the property (currently bug: they do)
        expect(output[0].items[0].rootLabel).toBeUndefined();
        expect(output[0].items[1].rootLabel).toBeUndefined();
    });

    it('should define property at scoped level when using in()', () => {
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ category: string, value: number }>()
                .groupBy(['category'], 'items')
                .in('items').defineProperty('itemLabel' as any, (item: any) => `Item: ${item.value}`)
        );

        pipeline.add("item1", { category: 'A', value: 10 });
        pipeline.add("item2", { category: 'A', value: 20 });

        const output = getOutput() as any[];
        expect(output.length).toBe(1);
        // Root level should NOT have the property
        expect(output[0].itemLabel).toBeUndefined();
        // Nested items SHOULD have the property
        expect(output[0].items[0].itemLabel).toBe('Item: 10');
        expect(output[0].items[1].itemLabel).toBe('Item: 20');
    });
});


