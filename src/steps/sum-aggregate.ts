import { CommutativeAggregateStep, type AddOperator, type SubtractOperator } from './commutative-aggregate';
import type { ImmutableProps, Step, TypeDescriptor } from '../pipeline';

/**
 * A step that computes the sum of a numeric property over items in a nested array.
 * 
 * - Handles null/undefined values by treating them as 0
 * - Returns 0 for empty arrays
 * - Uses CommutativeAggregateStep internally for incremental updates
 */
export class SumAggregateStep<
    TInput,
    TPath extends string[],
    TPropertyName extends string
> implements Step {
    
    private aggregateStep: CommutativeAggregateStep<TInput, TPath, TPropertyName, number>;
    
    constructor(
        private input: Step,
        private arrayPath: TPath,
        private propertyName: TPropertyName,
        private numericProperty: string
    ) {
        const addOp: AddOperator<ImmutableProps, number> = (acc, item) => {
            const value = item[this.numericProperty];
            const numValue = (value === null || value === undefined) ? 0 : Number(value);
            return (acc ?? 0) + numValue;
        };
        
        const subtractOp: SubtractOperator<ImmutableProps, number> = (acc, item) => {
            const value = item[this.numericProperty];
            const numValue = (value === null || value === undefined) ? 0 : Number(value);
            return acc - numValue;
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
