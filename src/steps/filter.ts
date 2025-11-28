import type { AddedHandler, ImmutableProps, RemovedHandler, ModifiedHandler, Step, TypeDescriptor } from '../pipeline';
import { pathsMatch } from '../util/path';

/**
 * A step that filters items based on a predicate function.
 * 
 * This is a STATELESS implementation - no item storage required because:
 * 1. Items are immutable
 * 2. RemovedHandler receives immutableProps
 * 3. Predicate re-evaluation is deterministic
 */
export class FilterStep<T> implements Step {
    constructor(
        private input: Step,
        private predicate: (item: T) => boolean,
        private scopePath: string[]
    ) {}

    getTypeDescriptor(): TypeDescriptor {
        return this.input.getTypeDescriptor();
    }

    onAdded(pathNames: string[], handler: AddedHandler): void {
        if (this.isAtScopePath(pathNames)) {
            this.input.onAdded(pathNames, (path, key, immutableProps) => {
                if (this.predicate(immutableProps as T)) {
                    handler(path, key, immutableProps);
                }
            });
        } else {
            this.input.onAdded(pathNames, handler);
        }
    }

    onRemoved(pathNames: string[], handler: RemovedHandler): void {
        if (this.isAtScopePath(pathNames)) {
            this.input.onRemoved(pathNames, (path, key, immutableProps) => {
                if (this.predicate(immutableProps as T)) {
                    handler(path, key, immutableProps);
                }
            });
        } else {
            this.input.onRemoved(pathNames, handler);
        }
    }

    onModified(pathNames: string[], handler: ModifiedHandler): void {
        this.input.onModified(pathNames, handler);
    }

    private isAtScopePath(pathNames: string[]): boolean {
        return pathsMatch(pathNames, this.scopePath);
    }
}