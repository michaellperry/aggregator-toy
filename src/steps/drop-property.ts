import type { Step } from '../pipeline';
import { getPathsFromDescriptor, type TypeDescriptor } from '../pipeline';

export class DropPropertyStep<T, K extends keyof T> implements Step<Omit<T, K>> {
    constructor(private input: Step<T>, private propertyName: K) {}
    
    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }

    getPaths(): string[][] {
        return getPathsFromDescriptor(this.getTypeDescriptor());
    }
    
    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: Omit<T, K>) => void): void {
        this.input.onAdded(path, (path, key, immutableProps) => {
            const { [this.propertyName]: _, ...rest } = immutableProps;
            handler(path, key, rest as Omit<T, K>);
        });
    }
    
    onRemoved(path: string[], handler: (path: string[], key: string) => void): void {
        this.input.onRemoved(path, handler);
    }
}

