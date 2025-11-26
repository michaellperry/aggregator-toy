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

export type ImmutableProps = {
    [key: string]: any;
};

export type AddedHandler = (path: string[], key: string, immutableProps: ImmutableProps) => void;

export type RemovedHandler = (path: string[], key: string) => void;

export type ModifiedHandler = (path: string[], key: string, name: string, value: any) => void;

export function getPathNamesFromDescriptor(descriptor: TypeDescriptor): string[][] {
    // Include the path to the root of the descriptor
    const paths: string[][] = [[]];
    // Recursively get paths from nested type descriptors
    for (const array of descriptor.arrays) {
        const allChildPaths = getPathNamesFromDescriptor(array.type);
        for (const childPath of allChildPaths) {
            paths.push([array.name, ...childPath]);
        }
    }
    return paths;
}

export interface Step {
    getTypeDescriptor(): TypeDescriptor;
    onAdded(path: string[], handler: AddedHandler): void;
    onRemoved(path: string[], handler: RemovedHandler): void;
    onModified(path: string[], handler: ModifiedHandler): void;
}

