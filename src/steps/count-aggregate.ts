import { CommutativeAggregateStep, type AddOperator, type SubtractOperator } from './commutative-aggregate';
import type { ImmutableProps, Step, TypeDescriptor } from '../pipeline';

/**
 * A step that counts items in a nested array.
 * 
 * - Returns 0 for empty arrays
 * - Increments/decrements correctly on add/remove
 * - Uses CommutativeAggregateStep internally for incremental updates
 */
export class CountAggregateStep<
    TInput,
    TPath extends string[],
    TPropertyName extends string
> implements Step {
    
    private aggregateStep: CommutativeAggregateStep<TInput, TPath, TPropertyName, number>;
    
    constructor(
        private input: Step,
        private arrayPath: TPath,
        private propertyName: TPropertyName
    ) {
        const addOp: AddOperator<ImmutableProps, number> = (acc, _item) => {
            return (acc ?? 0) + 1;
        };
        
        const subtractOp: SubtractOperator<ImmutableProps, number> = (acc, _item) => {
            return acc - 1;
        };
        
        this.aggregateStep = new CommutativeAggregateStep(
            this.input,
            this.arrayPath,
            this.propertyName,
            { add: addOp, subtract: subtractOp }
        );
    }
    
    getTypeDescriptor(): TypeDescriptor {
        return this.aggregateStep.getTypeDescriptor();
    }
    
    onAdded(pathNames: string[], handler: (path: string[], key: string, immutableProps: ImmutableProps) => void): void {
        this.aggregateStep.onAdded(pathNames, handler);
    }
    
    onRemoved(pathNames: string[], handler: (path: string[], key: string) => void): void {
        this.aggregateStep.onRemoved(pathNames, handler);
    }
    
    onModified(pathNames: string[], handler: (path: string[], key: string, name: string, value: any) => void): void {
        this.aggregateStep.onModified(pathNames, handler);
    }
}
