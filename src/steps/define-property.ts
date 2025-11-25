import type { Step } from '../pipeline';

export class DefinePropertyStep<T, K extends string, U> implements Step<T & Record<K, U>> {
    constructor(private input: Step<T>, private propertyName: K, private compute: (item: T) => U) {}
    
    getPaths(): string[][] {
        return this.input.getPaths();
    }
    
    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: T & Record<K, U>) => void): void {
        this.input.onAdded(path, (path, key, immutableProps) => {
            handler(path, key, { ...immutableProps, [this.propertyName]: this.compute(immutableProps) } as T & Record<K, U>);
        });
    }
    onRemoved(path: string[], handler: (path: string[], key: string) => void): void {
        this.input.onRemoved(path, handler);
    }
}

