import type { ImmutableProps, Step } from '../pipeline';
import { getPathsFromDescriptor, type TypeDescriptor } from '../pipeline';

export class GroupByStep<T extends {}, K extends keyof T, ArrayName extends string> implements Step {
    groupAddedHandlers: any;
    itemAddedHandlers: any;

    constructor(
        private input: Step,
        private keyProperties: K[],
        private arrayName: ArrayName
    ) {
        // Register with the input step to receive items at the root path
        this.input.onAdded([], (path, key, immutableProps) => {
            this.handleAdded(path, key, immutableProps);
        });
        this.input.onRemoved([], (path, key) => {
            this.handleRemoved(path, key);
        });
    }

    getTypeDescriptor(): TypeDescriptor {
        const inputDescriptor = this.input.getTypeDescriptor();
        return {
            arrays: [
                {
                    name: this.arrayName,
                    type: inputDescriptor  // Items have the input type
                }
            ]
        };
    }

    getPaths(): string[][] {
        return getPathsFromDescriptor(this.getTypeDescriptor());
    }

    onAdded(path: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        if (path.length === 0) {
            // Handler is at the group level
            this.groupAddedHandlers.push(handler);
        } else if (path.length === 1 && path[0] === this.arrayName) {
            // Handler is at the item level
            this.itemAddedHandlers.push(handler);
        } else if (path.length > 1 && path[0] === this.arrayName) {
            // Handler is below this array in the tree
        } else {
            this.input.onAdded(path, handler);
        }
    }

    onRemoved(path: string[], handler: (path: string[], key: string) => void): void {
    }

    private handleAdded<T>(path: string[], key: string, immutableProps: T) {
        if (path.length !== 0) {
            throw new Error("GroupByStep notified of item added at a different level");
        }
        throw new Error("Method not implemented.");
    }

    private handleRemoved(path: string[], key: string) {
        if (path.length !== 0) {
            throw new Error("GroupByStep notified of item removed at a different level");
        }
        throw new Error("Method not implemented.");
    }
}

