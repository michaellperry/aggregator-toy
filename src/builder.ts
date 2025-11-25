import type { Pipeline, Step } from './pipeline';
import { DefinePropertyStep } from './steps/define-property';
import { DropPropertyStep } from './steps/drop-property';
import { GroupByStep } from './steps/group-by';
import { validateArrayName } from './util/validation';

// Public types (exported for use in build() signature)
export type KeyedArray<T> = { key: string, value: T }[];
export type Transform<T> = (state: T) => T;

export class PipelineBuilder<TStart, T extends {}> {
    constructor(private input: Pipeline<TStart>, private lastStep: Step<T>) {}

    defineProperty<K extends string, U>(propertyName: K, compute: (item: T) => U): PipelineBuilder<TStart, T & Record<K, U>> {
        const newStep = new DefinePropertyStep(this.lastStep, propertyName, compute);
        return new PipelineBuilder<TStart, T & Record<K, U>>(this.input, newStep);
    }

    dropProperty<K extends keyof T>(propertyName: K): PipelineBuilder<TStart, Omit<T, K>> {
        const newStep = new DropPropertyStep(this.lastStep, propertyName);
        return new PipelineBuilder<TStart, Omit<T, K>>(this.input, newStep);
    }

    groupBy<K extends keyof T, ArrayName extends string>(
        keyProperties: K[],
        arrayName: ArrayName
    ): PipelineBuilder<TStart, Pick<T, K> & Record<ArrayName, KeyedArray<Omit<T, K>>>> {
        validateArrayName(arrayName);
        const newStep = new GroupByStep(this.lastStep, keyProperties, arrayName);
        return new PipelineBuilder<TStart, Pick<T, K> & Record<ArrayName, KeyedArray<Omit<T, K>>>>(this.input, newStep);
    }

    build(setState: (transform: Transform<KeyedArray<T>>) => void): Pipeline<TStart> {
        const paths = this.lastStep.getPaths();
        
        // Register handlers for each path the step will emit
        paths.forEach(path => {
            this.lastStep.onAdded(path, (path, key, immutableProps) => {
                setState(state => {
                    const existingIndex = state.findIndex(item => item.key === key);
                    if (existingIndex >= 0) {
                        // Update existing item
                        const updated = [...state];
                        updated[existingIndex] = { key, value: immutableProps };
                        return updated;
                    } else {
                        // Add new item
                        return [...state, { key, value: immutableProps }];
                    }
                });
            });
            
            this.lastStep.onRemoved(path, (path, key) => {
                setState(state => state.filter(item => item.key !== key));
            });
        });
        
        return this.input;
    }
}

