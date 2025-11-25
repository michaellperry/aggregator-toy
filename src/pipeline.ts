export interface Pipeline<T> {
    add(key: string, immutableProps: T): void;
    remove(key: string): void;
}

export interface Step<T> {
    onAdded(handler: (key: string, immutableProps: T) => void): void;
    onRemoved(handler: (key: string) => void): void;
}

