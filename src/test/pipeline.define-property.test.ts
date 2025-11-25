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
});


