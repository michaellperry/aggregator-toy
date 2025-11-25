export interface Pipeline<T> {
    add(key: string, immutableProps: T): void;
    remove(key: string): void;
}

export interface Step<T> {
    getPaths(): string[][];
    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: T) => void): void;
    onRemoved(path: string[], handler: (path: string[], key: string) => void): void;
}

