import type { ImmutableProps, Step } from '../pipeline';
import { getPathNamesFromDescriptor, type TypeDescriptor } from '../pipeline';

export class DropPropertyStep<T, K extends keyof T> implements Step {
    constructor(private input: Step, private propertyName: K) {}
    
    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }
    
    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        this.input.onAdded(path, (path, key, immutableProps) => {
            const { [this.propertyName]: _, ...rest } = immutableProps;
            handler(path, key, rest as Omit<T, K>);
        });
    }
    
    onRemoved(path: string[], handler: (path: string[], key: string) => void): void {
        this.input.onRemoved(path, handler);
    }
}

