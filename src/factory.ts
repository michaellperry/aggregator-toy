import type { Pipeline, Step } from './pipeline';
import { PipelineBuilder } from './builder';

// Private class (not exported)
class InputPipeline<T> implements Pipeline<T>, Step<T> {
    private handlers: ((key: string, immutableProps: T) => void)[] = [];

    add(key: string, immutableProps: T): void {
        this.handlers.forEach(handler => handler(key, immutableProps));
    }

    onAdded(handler: (key: string, immutableProps: T) => void): void {
        this.handlers.push(handler);
    }
}

export function createPipeline<TStart>(): PipelineBuilder<TStart, TStart> {
    const start = new InputPipeline<TStart>();
    return new PipelineBuilder<TStart, TStart>(start, start);
}

