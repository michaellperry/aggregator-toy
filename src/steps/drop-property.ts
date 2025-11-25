import type { Step } from '../pipeline';

export class DropPropertyStep<T, K extends keyof T> implements Step<Omit<T, K>> {
    constructor(private input: Step<T>, private propertyName: K) {}
    
    onAdded(handler: (path: string[], key: string, immutableProps: Omit<T, K>) => void): void {
        this.input.onAdded((path, key, immutableProps) => {
            const { [this.propertyName]: _, ...rest } = immutableProps;
            handler(path, key, rest as Omit<T, K>);
        });
    }
    
    onRemoved(handler: (path: string[], key: string) => void): void {
        this.input.onRemoved(handler);
    }
}

