import type { ImmutableProps, Step } from '../pipeline';
import { getPathsFromDescriptor, type TypeDescriptor } from '../pipeline';

export class DefinePropertyStep<T, K extends string, U> implements Step {
    constructor(private input: Step, private propertyName: K, private compute: (item: T) => U) {}
    
    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }

    getPaths(): string[][] {
        return getPathsFromDescriptor(this.getTypeDescriptor());
    }
    
    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        this.input.onAdded(path, (path, key, immutableProps) => {
            handler(path, key, { ...immutableProps, [this.propertyName]: this.compute(immutableProps as T) } as T & Record<K, U>);
        });
    }
    onRemoved(path: string[], handler: (path: string[], key: string) => void): void {
        this.input.onRemoved(path, handler);
    }
}

