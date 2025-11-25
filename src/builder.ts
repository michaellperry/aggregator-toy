import type { Pipeline, Step } from './pipeline';
import { DefinePropertyStep } from './steps/define-property';

// Public types (exported for use in build() signature)
export type KeyedArray<T> = { key: string, value: T }[];
export type Transform<T> = (state: T) => T;

export class PipelineBuilder<TStart, T> {
    constructor(private input: Pipeline<TStart>, private lastStep: Step<T>) {}

    defineProperty<K extends string, U>(propertyName: K, compute: (item: T) => U): PipelineBuilder<TStart, T & Record<K, U>> {
        const newStep = new DefinePropertyStep(this.lastStep, propertyName, compute);
        return new PipelineBuilder<TStart, T & Record<K, U>>(this.input, newStep);
    }

    build(setState: (transform: Transform<KeyedArray<T>>) => void): Pipeline<TStart> {
        this.lastStep.onAdded((key, immutableProps) => {
            setState(state => [...state, { key, value: immutableProps }]);
        });
        this.lastStep.onRemoved((key) => {
            setState(state => state.filter(item => item.key !== key));
        });
        return this.input;
    }
}

