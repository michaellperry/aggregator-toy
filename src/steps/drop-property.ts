import type { ImmutableProps, Step } from '../pipeline';
import { type TypeDescriptor } from '../pipeline';
import { pathsMatch } from '../util/path';

export class DropPropertyStep<T, K extends keyof T> implements Step {
    constructor(
        private input: Step,
        private propertyName: K,
        private scopePath: string[]
    ) {}
    
    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }
    
    onAdded(pathNames: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        if (this.isAtScopePath(pathNames)) {
            // Drop the property
            this.input.onAdded(pathNames, (path, key, immutableProps) => {
                const { [this.propertyName]: _, ...rest } = immutableProps;
                handler(path, key, rest as Omit<T, K>);
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

