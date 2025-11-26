import { getPathNamesFromDescriptor, type ImmutableProps, type Pipeline, type Step, type TypeDescriptor } from './pipeline';
import { DefinePropertyStep } from './steps/define-property';
import { DropPropertyStep } from './steps/drop-property';
import { GroupByStep } from './steps/group-by';

// Public types (exported for use in build() signature)
export type KeyedArray<T> = { key: string, value: T }[];
export type Transform<T> = (state: T) => T;

// Type utility to expand intersection types into a single object type for better IDE display
type Expand<T> = T extends infer O ? { [K in keyof O]: O[K] } : never;

export class PipelineBuilder<TStart, T extends {}> {
    constructor(private input: Pipeline<TStart>, private lastStep: Step) {}

    defineProperty<K extends string, U>(propertyName: K, compute: (item: T) => U): PipelineBuilder<TStart, T & Record<K, U>> {
        const newStep = new DefinePropertyStep(this.lastStep, propertyName, compute);
        return new PipelineBuilder<TStart, T & Record<K, U>>(this.input, newStep);
    }

    dropProperty<K extends keyof T>(propertyName: K): PipelineBuilder<TStart, Omit<T, K>> {
        const newStep = new DropPropertyStep<T, K>(this.lastStep, propertyName);
        return new PipelineBuilder<TStart, Omit<T, K>>(this.input, newStep);
    }

    groupBy<K extends keyof T, ArrayName extends string>(
        keyProperties: K[],
        arrayName: ArrayName
    ): PipelineBuilder<TStart, Expand<{
        [P in K]: T[P]
    } & {
        [P in ArrayName]: KeyedArray<{
            [Q in Exclude<keyof T, K>]: T[Q]
        }>
    }>> {
        const newStep = new GroupByStep<T, K, ArrayName>(this.lastStep, keyProperties, arrayName);
        return new PipelineBuilder<TStart, Expand<{
            [P in K]: T[P]
        } & {
            [P in ArrayName]: KeyedArray<{
                [Q in Exclude<keyof T, K>]: T[Q]
            }>
        }>>(this.input, newStep);
    }

    getTypeDescriptor(): TypeDescriptor {
        return this.lastStep.getTypeDescriptor();
    }

    build(setState: (transform: Transform<KeyedArray<T>>) => void, typeDescriptor: TypeDescriptor): Pipeline<TStart> {
        const pathNames = getPathNamesFromDescriptor(typeDescriptor);
        
        // Register handlers for each path the step will emit
        pathNames.forEach(pathName => {
            this.lastStep.onAdded(pathName, (path, key, immutableProps) => {
                setState(state => addToKeyedArray(state, pathName, path, key, immutableProps) as KeyedArray<T>);
            });
            
            this.lastStep.onRemoved(pathName, (path, key) => {
                setState(state => removeFromKeyedArray(state, pathName, path, key) as KeyedArray<T>);
            });
            
            this.lastStep.onModified(pathName, (path, key, name, value) => {
                setState(state => modifyInKeyedArray(state, pathName, path, key, name, value) as KeyedArray<T>);
            });
        });
        
        return this.input;
    }
}

function addToKeyedArray(state: KeyedArray<any>, pathName: string[], path: string[], key: string, immutableProps: ImmutableProps): KeyedArray<any> {
    if (pathName.length === 0) {
        if (path.length !== 0) {
            throw new Error("Mismatched path length when setting state");
        }
        return [...state, { key, value: immutableProps }];
    }
    else {
        if (path.length === 0) {
            throw new Error("Mismatched path length when setting state");
        }
        const parentKey = path[0];
        const arrayName = pathName[0];
        const existingItemIndex = state.findIndex(item => item.key === parentKey);
        if (existingItemIndex < 0) {
            throw new Error("Path references unknown item when setting state");
        }
        const existingItem = state[existingItemIndex];
        const existingArray = existingItem.value[arrayName] as KeyedArray<any> || [];
        const modifiedArray = addToKeyedArray(existingArray, pathName.slice(1), path.slice(1), key, immutableProps);
        const modifiedItem = {
            key: parentKey,
            value: {
                ...existingItem.value,
                [pathName[0]]: modifiedArray
            }
        };
        return [
            ...state.slice(0, existingItemIndex),
            modifiedItem,
            ...state.slice(existingItemIndex+1)
        ];
    }
}

function removeFromKeyedArray(state: KeyedArray<any>, pathName: string[], path: string[], key: string): KeyedArray<any> {
    if (pathName.length === 0) {
        if (path.length !== 0) {
            throw new Error("Mismatched path length when removing from state");
        }
        return state.filter(item => item.key !== key);
    }
    else {
        if (path.length === 0) {
            throw new Error("Mismatched path length when removing from state");
        }
        const parentKey = path[0];
        const arrayName = pathName[0];
        const existingItemIndex = state.findIndex(item => item.key === parentKey);
        if (existingItemIndex < 0) {
            throw new Error("Path references unknown item when removing from state");
        }
        const existingItem = state[existingItemIndex];
        const existingArray = existingItem.value[arrayName] as KeyedArray<any> || [];
        const modifiedArray = removeFromKeyedArray(existingArray, pathName.slice(1), path.slice(1), key);
        const modifiedItem = {
            key: parentKey,
            value: {
                ...existingItem.value,
                [pathName[0]]: modifiedArray
            }
        };
        return [
            ...state.slice(0, existingItemIndex),
            modifiedItem,
            ...state.slice(existingItemIndex+1)
        ];
    }
}

function modifyInKeyedArray(state: KeyedArray<any>, pathName: string[], path: string[], key: string, name: string, value: any): KeyedArray<any> {
    if (pathName.length === 0) {
        if (path.length !== 0) {
            throw new Error("Mismatched path length when modifying state");
        }
        const existingItemIndex = state.findIndex(item => item.key === key);
        if (existingItemIndex < 0) {
            throw new Error("Path references unknown item when modifying state");
        }
        const existingItem = state[existingItemIndex];
        const modifiedItem = {
            key: key,
            value: {
                ...existingItem.value,
                [name]: value
            }
        };
        return [
            ...state.slice(0, existingItemIndex),
            modifiedItem,
            ...state.slice(existingItemIndex+1)
        ];
    }
    else {
        if (path.length === 0) {
            throw new Error("Mismatched path length when modifying state");
        }
        const parentKey = path[0];
        const arrayName = pathName[0];
        const existingItemIndex = state.findIndex(item => item.key === parentKey);
        if (existingItemIndex < 0) {
            throw new Error("Path references unknown item when modifying state");
        }
        const existingItem = state[existingItemIndex];
        const existingArray = existingItem.value[arrayName] as KeyedArray<any> || [];
        const modifiedArray = modifyInKeyedArray(existingArray, pathName.slice(1), path.slice(1), key, name, value);
        const modifiedItem = {
            key: parentKey,
            value: {
                ...existingItem.value,
                [pathName[0]]: modifiedArray
            }
        };
        return [
            ...state.slice(0, existingItemIndex),
            modifiedItem,
            ...state.slice(existingItemIndex+1)
        ];
    }
}

