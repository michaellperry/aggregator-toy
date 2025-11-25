import type { Step } from '../pipeline';

export class DefinePropertyStep<T, K extends string, U> implements Step<T & Record<K, U>> {
    constructor(private input: Step<T>, private propertyName: K, private compute: (item: T) => U) {}
    onAdded(handler: (key: string, immutableProps: T & Record<K, U>) => void): void {
        this.input.onAdded((key, immutableProps) => {
            handler(key, { ...immutableProps, [this.propertyName]: this.compute(immutableProps) } as T & Record<K, U>);
        });
    }
    onRemoved(handler: (key: string) => void): void {
        this.input.onRemoved(handler);
    }
}

