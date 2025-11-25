export interface Pipeline<T> {
    add(key: string, immutableProps: T): void;
    remove(key: string): void;
}

export interface TypeDescriptor {
    arrays: ArrayDescriptor[];
}

export interface ArrayDescriptor {
    name: string;
    type: TypeDescriptor;
}

export function getPathsFromDescriptor(descriptor: TypeDescriptor): string[][] {
    return [
        [],  // Always have empty path for groups
        ...descriptor.arrays.map(arr => [arr.name])  // One path per array
    ];
}

export interface Step<T> {
    getPaths(): string[][];
    getTypeDescriptor(): TypeDescriptor;
    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: T) => void): void;
    onRemoved(path: string[], handler: (path: string[], key: string) => void): void;
}

