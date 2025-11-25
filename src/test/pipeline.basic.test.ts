import { createPipeline } from '../index';
import { createTestPipeline } from './helpers';

describe('pipeline basic operations', () => {
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
});


