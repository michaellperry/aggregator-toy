import { createPipeline } from '../index';

describe('pipeline getPaths', () => {
    it('should return [[]] for InputPipeline', () => {
        const pipeline = createPipeline<{ message: string }>();
        // Access the internal step through the builder
        const builder = pipeline as any;
        const step = builder.lastStep;
        expect(step.getPathNames()).toEqual([[]]);
    });

    it('should pass through paths for DefinePropertyStep', () => {
        const pipeline = createPipeline<{ a: number }>()
            .defineProperty('b', () => 1);
        const builder = pipeline as any;
        const step = builder.lastStep;
        expect(step.getPathNames()).toEqual([[]]);
    });

    it('should pass through paths for DropPropertyStep', () => {
        const pipeline = createPipeline<{ a: number, b: number }>()
            .dropProperty('b');
        const builder = pipeline as any;
        const step = builder.lastStep;
        expect(step.getPathNames()).toEqual([[]]);
    });

    it('should return [[], [arrayName]] for GroupByStep', () => {
        const pipeline = createPipeline<{ category: string, value: number }>()
            .groupBy(['category'], 'items');
        const builder = pipeline as any;
        const step = builder.lastStep;
        expect(step.getPathNames()).toEqual([[], ['items']]);
    });

    it('should pass through paths when GroupByStep is chained with other steps', () => {
        const pipeline = createPipeline<{ category: string, value: number }>()
            .defineProperty('computed', () => 1)
            .groupBy(['category'], 'items')
            .dropProperty('computed' as any);
        const builder = pipeline as any;
        const step = builder.lastStep;
        // DropPropertyStep should pass through the paths from GroupByStep
        expect(step.getPathNames()).toEqual([[], ['items']]);
    });
});


