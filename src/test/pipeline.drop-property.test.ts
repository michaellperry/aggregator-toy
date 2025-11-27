import { createPipeline } from '../index';
import { createTestPipeline } from './helpers';

describe('pipeline dropProperty', () => {
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

    it('should only drop property at scoped level, not at nested levels', () => {
        // Create pipeline with property defined at both root and nested levels
        const [pipeline, getOutput] = createTestPipeline(() => 
            createPipeline<{ category: string, value: number }>()
                .groupBy(['category'], 'items')
                .defineProperty('computed', (group) => `Group: ${group.category}`)
                .in('items').defineProperty('computed', (item) => `Item: ${item.value}`)
                .in('items').dropProperty('computed')  // Drop at nested level - should only drop from items, not groups
        );

        pipeline.add("item1", { category: 'A', value: 10 });
        pipeline.add("item2", { category: 'A', value: 20 });

        const output = getOutput();
        expect(output.length).toBe(1);
        // Root level should still have computed property (not dropped)
        expect(output[0].computed).toBe('Group: A');
        // Nested level should not have computed property (dropped)
        expect(output[0].items[0] && 'computed' in output[0].items[0] ? (output[0].items[0] as { computed?: unknown }).computed : undefined).toBeUndefined();
        expect(output[0].items[1] && 'computed' in output[0].items[1] ? (output[0].items[1] as { computed?: unknown }).computed : undefined).toBeUndefined();
    });
});


