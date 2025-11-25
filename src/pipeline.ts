export interface Pipeline<T> {
    add(key: string, immutableProps: T): void;
}

export interface Step<T> {
    onAdded(handler: (key: string, immutableProps: T) => void): void;
}

