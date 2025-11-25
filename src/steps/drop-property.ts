import type { Step } from '../pipeline';

export class DropPropertyStep<T, K extends keyof T> implements Step<Omit<T, K>> {
    constructor(private input: Step<T>, private propertyName: K) {}
    
    onAdded(handler: (key: string, immutableProps: Omit<T, K>) => void): void {
        this.input.onAdded((key, immutableProps) => {
            const { [this.propertyName]: _, ...rest } = immutableProps;
            handler(key, rest as Omit<T, K>);
        });
    }
    
    onRemoved(handler: (key: string) => void): void {
        this.input.onRemoved(handler);
    }
}

