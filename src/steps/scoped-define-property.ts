import type { ImmutableProps, Step, TypeDescriptor } from '../pipeline';
import { pathsMatch } from '../util/path';

/**
 * A step that applies defineProperty only to items at a specific scope path.
 * 
 * This step wraps a compute function and only applies it to items
 * that are at the specified scope path, passing through items at
 * other paths unchanged.
 */
export class ScopedDefinePropertyStep<T, K extends string, U> implements Step {
    constructor(
        private input: Step,
        private propertyName: K,
        private compute: (item: unknown) => U,
        private scopePath: string[]
    ) {}
    
    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }
    
    onAdded(pathNames: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        if (this.isAtScopePath(pathNames)) {
            // Apply the property transformation at the scoped level
            this.input.onAdded(pathNames, (path, key, immutableProps) => {
                handler(path, key, { ...immutableProps, [this.propertyName]: this.compute(immutableProps) });
            });
        } else {
            // Pass through unchanged
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