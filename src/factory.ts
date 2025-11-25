import type { Pipeline, Step } from './pipeline';
import { PipelineBuilder } from './builder';

// Private class (not exported)
class InputPipeline<T> implements Pipeline<T>, Step<T> {
    private handlers: ((key: string, immutableProps: T) => void)[] = [];
    private removalHandlers: ((key: string) => void)[] = [];

    add(key: string, immutableProps: T): void {
        this.handlers.forEach(handler => handler(key, immutableProps));
    }

    remove(key: string): void {
        this.removalHandlers.forEach(handler => handler(key));
    }

    onAdded(handler: (key: string, immutableProps: T) => void): void {
        this.handlers.push(handler);
    }

    onRemoved(handler: (key: string) => void): void {
        this.removalHandlers.push(handler);
    }
}

export function createPipeline<TStart>(): PipelineBuilder<TStart, TStart> {
    const start = new InputPipeline<TStart>();
    return new PipelineBuilder<TStart, TStart>(start, start);
}

