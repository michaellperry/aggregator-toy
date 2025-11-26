import type { ImmutableProps, Step } from '../pipeline';
import { type TypeDescriptor } from '../pipeline';

export class DefinePropertyStep<T, K extends string, U> implements Step {
    constructor(private input: Step, private propertyName: K, private compute: (item: T) => U) {}
    
    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }
    
    onAdded(pathNames: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        this.input.onAdded(pathNames, (path, key, immutableProps) => {
            handler(path, key, { ...immutableProps, [this.propertyName]: this.compute(immutableProps as T) } as T & Record<K, U>);
        });
    }
    onRemoved(pathNames: string[], handler: (path: string[], key: string) => void): void {
        this.input.onRemoved(pathNames, handler);
    }

    onModified(pathNames: string[], handler: (path: string[], key: string, name: string, value: any) => void): void {
        this.input.onModified(pathNames, handler);
    }
}

