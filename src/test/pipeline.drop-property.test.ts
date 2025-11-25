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
});

