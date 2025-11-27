import type { ImmutableProps, Step } from '../pipeline';
import { type TypeDescriptor } from '../pipeline';
import { pathsMatch } from '../util/path';

export class DefinePropertyStep<T, K extends string, U> implements Step {
    constructor(
        private input: Step,
        private propertyName: K,
        private compute: (item: T) => U,
        private scopePath: string[]
    ) {}
    
    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }
    
    onAdded(pathNames: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        if (this.isAtScopePath(pathNames)) {
            // Apply the property transformation at the scoped level
            this.input.onAdded(pathNames, (path, key, immutableProps) => {
                handler(path, key, { ...immutableProps, [this.propertyName]: this.compute(immutableProps as T) } as T & Record<K, U>);
            });
        } else {
            // Pass through unchanged when not at scope path
            this.input.onAdded(pathNames, handler);
        }
    }
    
    onRemoved(pathNames: string[], handler: (path: string[], key: string) => void): void {
        this.input.onRemoved(pathNames, handler);
    }

    onModified(pathNames: string[], handler: (path: string[], key: string, name: string, value: any) => void): void {
        this.input.onModified(pathNames, handler);
    }
    
    private isAtScopePath(pathNames: string[]): boolean {
        return pathsMatch(pathNames, this.scopePath);
    }
}

