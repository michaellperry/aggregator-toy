export interface Pipeline<T> {
    add(key: string, immutableProps: T): void;
    remove(key: string): void;
}

export interface Step<T> {
    onAdded(handler: (path: string[], key: string, immutableProps: T) => void): void;
    onRemoved(handler: (path: string[], key: string) => void): void;
}

