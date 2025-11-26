import type { ImmutableProps, Pipeline, Step } from './pipeline';
import { PipelineBuilder } from './builder';
import { getPathsFromDescriptor, type TypeDescriptor } from './pipeline';

// Private class (not exported)
class InputPipeline<T> implements Pipeline<T>, Step {
    private handlers: Map<string, ((path: string[], key: string, immutableProps: ImmutableProps) => void)[]> = new Map();
    private removalHandlers: Map<string, ((path: string[], key: string) => void)[]> = new Map();

    getTypeDescriptor(): TypeDescriptor {
        return { arrays: [] }; // No arrays at input level
    }

    getPaths(): string[][] {
        return getPathsFromDescriptor(this.getTypeDescriptor());
    }

    add(key: string, immutableProps: T): void {
        const pathKey = [].join(':');
        const handlersForPath = this.handlers.get(pathKey) || [];
        handlersForPath.forEach(handler => handler([], key, immutableProps as ImmutableProps));
    }

    remove(key: string): void {
        const pathKey = [].join(':');
        const handlersForPath = this.removalHandlers.get(pathKey) || [];
        handlersForPath.forEach(handler => handler([], key));
    }

    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        const pathKey = path.join(':');
        const handlersForPath = this.handlers.get(pathKey) || [];
        handlersForPath.push(handler);
        this.handlers.set(pathKey, handlersForPath);
    }

    onRemoved(path: string[], handler: (path: string[], key: string) => void): void {
        const pathKey = path.join(':');
        const handlersForPath = this.removalHandlers.get(pathKey) || [];
        handlersForPath.push(handler);
        this.removalHandlers.set(pathKey, handlersForPath);
    }
}

export function createPipeline<TStart extends {}>(): PipelineBuilder<TStart, TStart> {
    const start = new InputPipeline<TStart>();
    return new PipelineBuilder<TStart, TStart>(start, start);
}

