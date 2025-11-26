import { createPipeline, TypeDescriptor } from '../index';
import { CommutativeAggregateStep, AddOperator, SubtractOperator } from '../steps/commutative-aggregate';
import { DropArrayStep } from '../steps/drop-array';
import type { Step, ImmutableProps } from '../pipeline';

// Helper type aliases for cleaner test code
type NumericAddOp = AddOperator<ImmutableProps, number>;
type NumericSubtractOp = SubtractOperator<ImmutableProps, number>;

describe('CommutativeAggregateStep', () => {
    describe('basic sum aggregation', () => {
        it('should emit aggregate via onModified', () => {
            // Set up: groupBy category, then aggregate items' prices
            const builder = createPipeline<{ category: string; itemName: string; price: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'totalPrice',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ path: string[]; key: string; name: string; value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ path: [...path], key, name, value });
            });
            
            // Trigger add via the input pipeline
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            inputPipeline.add('item1', { category: 'Electronics', itemName: 'Phone', price: 500 });
            
            // Verify aggregate was emitted via onModified
            expect(modifiedEvents.length).toBe(1);
            expect(modifiedEvents[0].name).toBe('totalPrice');
            expect(modifiedEvents[0].value).toBe(500);
        });

        it('should emit immutable props via onAdded WITHOUT aggregate', () => {
            const builder = createPipeline<{ category: string; itemName: string; price: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'totalPrice',
                { add: addOp, subtract: subtractOp }
            );
            
            const addedEvents: Array<{ path: string[]; key: string; props: ImmutableProps }> = [];
            aggregateStep.onAdded([], (path, key, props) => {
                addedEvents.push({ path: [...path], key, props: { ...props } });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            inputPipeline.add('item1', { category: 'Electronics', itemName: 'Phone', price: 500 });
            
            // Verify immutable props are passed through onAdded
            expect(addedEvents.length).toBe(1);
            expect(addedEvents[0].props).toHaveProperty('category', 'Electronics');
            
            // CRITICAL: Verify aggregate is NOT in onAdded props
            expect(addedEvents[0].props).not.toHaveProperty('totalPrice');
        });

        it('should accumulate sum when multiple items are added', () => {
            const builder = createPipeline<{ category: string; itemName: string; price: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'totalPrice',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            inputPipeline.add('item1', { category: 'Electronics', itemName: 'Phone', price: 500 });
            inputPipeline.add('item2', { category: 'Electronics', itemName: 'Laptop', price: 1200 });
            inputPipeline.add('item3', { category: 'Electronics', itemName: 'Tablet', price: 300 });
            
            // Verify aggregates: 500, then 1700, then 2000
            expect(modifiedEvents.length).toBe(3);
            expect(modifiedEvents[0].value).toBe(500);
            expect(modifiedEvents[1].value).toBe(1700);
            expect(modifiedEvents[2].value).toBe(2000);
        });
    });

    describe('subtract functionality', () => {
        it('should reduce aggregate when items are removed', () => {
            const builder = createPipeline<{ category: string; itemName: string; price: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'totalPrice',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value });
            });
            
            const inputPipeline = builder['input'] as { 
                add: (key: string, props: any) => void;
                remove: (key: string) => void;
            };
            
            inputPipeline.add('item1', { category: 'Electronics', itemName: 'Phone', price: 500 });
            inputPipeline.add('item2', { category: 'Electronics', itemName: 'Laptop', price: 1200 });
            
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(1700);
            
            // Remove item1 (price: 500)
            inputPipeline.remove('item1');
            
            // Verify aggregate reduced
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(1200);
        });

        it('should emit onModified when item is removed', () => {
            const builder = createPipeline<{ category: string; price: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'total',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ name: string; value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ name, value });
            });
            
            const inputPipeline = builder['input'] as { 
                add: (key: string, props: any) => void;
                remove: (key: string) => void;
            };
            
            inputPipeline.add('item1', { category: 'A', price: 100 });
            inputPipeline.add('item2', { category: 'A', price: 200 });
            
            const countBeforeRemove = modifiedEvents.length;
            inputPipeline.remove('item1');
            
            // A new modified event should be emitted
            expect(modifiedEvents.length).toBe(countBeforeRemove + 1);
            expect(modifiedEvents[modifiedEvents.length - 1].name).toBe('total');
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(200);
        });

        it('should handle removal of all items', () => {
            const builder = createPipeline<{ category: string; price: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'total',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value });
            });
            
            const inputPipeline = builder['input'] as { 
                add: (key: string, props: any) => void;
                remove: (key: string) => void;
            };
            
            inputPipeline.add('item1', { category: 'A', price: 100 });
            inputPipeline.add('item2', { category: 'A', price: 200 });
            
            inputPipeline.remove('item1');
            inputPipeline.remove('item2');
            
            // Final aggregate should be 0
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(0);
        });
    });

    describe('multiple parents (scoped aggregation)', () => {
        it('should maintain independent aggregate state per parent', () => {
            const builder = createPipeline<{ category: string; itemName: string; price: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'totalPrice',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ key: string; value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ key, value });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            
            // Add items to different categories
            inputPipeline.add('item1', { category: 'Electronics', itemName: 'Phone', price: 500 });
            inputPipeline.add('item2', { category: 'Clothing', itemName: 'Shirt', price: 50 });
            inputPipeline.add('item3', { category: 'Electronics', itemName: 'Laptop', price: 1200 });
            inputPipeline.add('item4', { category: 'Clothing', itemName: 'Pants', price: 80 });
            
            // Verify Electronics aggregate: 500 + 1200 = 1700
            const lastElectronics = modifiedEvents.find(e => e.value === 1700);
            expect(lastElectronics).toBeDefined();
            
            // Verify Clothing aggregate: 50 + 80 = 130
            const lastClothing = modifiedEvents.find(e => e.value === 130);
            expect(lastClothing).toBeDefined();
        });

        it('should not affect other parents when removing from one parent', () => {
            const builder = createPipeline<{ department: string; employee: string; salary: number }>()
                .groupBy(['department'], 'employees');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).salary;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).salary;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['employees'],
                'totalSalary',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ key: string; value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ key, value });
            });
            
            const inputPipeline = builder['input'] as { 
                add: (key: string, props: any) => void;
                remove: (key: string) => void;
            };
            
            // Add employees to two departments
            inputPipeline.add('emp1', { department: 'Engineering', employee: 'Alice', salary: 100000 });
            inputPipeline.add('emp2', { department: 'Sales', employee: 'Bob', salary: 80000 });
            inputPipeline.add('emp3', { department: 'Engineering', employee: 'Carol', salary: 120000 });
            
            // Remove from Engineering
            inputPipeline.remove('emp1');
            
            // Check Sales total is unchanged (80000)
            // Check Engineering total is now 120000
            const allValues = modifiedEvents.map(e => e.value);
            
            // 80000 should still be present (Sales unchanged)
            expect(allValues).toContain(80000);
            // Engineering should have been reduced from 220000 to 120000
            expect(allValues).toContain(220000); // Engineering: 100000 + 120000
            expect(allValues).toContain(120000); // Engineering after removal
        });
    });

    describe('nested array path', () => {
        it('should navigate through nested arrays correctly', () => {
            // Set up: state > city > venues
            const builder = createPipeline<{ state: string; city: string; venue: string; capacity: number }>()
                .groupBy(['state', 'city'], 'venues')
                .groupBy(['state'], 'cities');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).capacity;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).capacity;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['cities', 'venues'],
                'totalCapacity',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ path: string[]; key: string; name: string; value: any }> = [];
            aggregateStep.onModified(['cities'], (path, key, name, value) => {
                modifiedEvents.push({ path: [...path], key, name, value });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            
            inputPipeline.add('v1', { state: 'TX', city: 'Dallas', venue: 'Stadium', capacity: 50000 });
            inputPipeline.add('v2', { state: 'TX', city: 'Dallas', venue: 'Arena', capacity: 20000 });
            
            // Verify aggregate updates for Dallas city
            expect(modifiedEvents.length).toBeGreaterThan(0);
            expect(modifiedEvents[modifiedEvents.length - 1].name).toBe('totalCapacity');
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(70000);
        });

        it('should maintain separate aggregates for each nested parent', () => {
            const builder = createPipeline<{ state: string; city: string; venue: string; capacity: number }>()
                .groupBy(['state', 'city'], 'venues')
                .groupBy(['state'], 'cities');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, _item) => (acc ?? 0) + 1;
            const subtractOp: NumericSubtractOp = (acc, _item) => acc - 1;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['cities', 'venues'],
                'venueCount',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ path: string[]; key: string; value: any }> = [];
            aggregateStep.onModified(['cities'], (path, key, name, value) => {
                modifiedEvents.push({ path: [...path], key, value });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            
            // Add venues to Dallas
            inputPipeline.add('v1', { state: 'TX', city: 'Dallas', venue: 'Stadium', capacity: 50000 });
            inputPipeline.add('v2', { state: 'TX', city: 'Dallas', venue: 'Arena', capacity: 20000 });
            
            // Add venues to Houston
            inputPipeline.add('v3', { state: 'TX', city: 'Houston', venue: 'Center', capacity: 18000 });
            
            // Verify Dallas reaches count 2
            expect(modifiedEvents.some(e => e.value === 2)).toBe(true);
            // Verify Houston reaches count 1
            expect(modifiedEvents.some(e => e.value === 1)).toBe(true);
        });
    });

    describe('initial undefined state', () => {
        it('should handle undefined as initial aggregate correctly', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            
            let receivedUndefined = false;
            const addOp: NumericAddOp = (acc, item) => {
                if (acc === undefined) {
                    receivedUndefined = true;
                }
                return (acc ?? 0) + (item as any).value;
            };
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).value;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'sum',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            inputPipeline.add('item1', { category: 'A', value: 10 });
            
            // Verify that the add function received undefined for first item
            expect(receivedUndefined).toBe(true);
            expect(modifiedEvents[0].value).toBe(10);
        });

        it('should receive undefined for first item of each new parent', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            
            const undefinedCount: { count: number } = { count: 0 };
            const addOp: NumericAddOp = (acc, item) => {
                if (acc === undefined) {
                    undefinedCount.count++;
                }
                return (acc ?? 0) + (item as any).value;
            };
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).value;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'sum',
                { add: addOp, subtract: subtractOp }
            );
            
            aggregateStep.onModified([], () => {});
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            
            // First item of category A
            inputPipeline.add('item1', { category: 'A', value: 10 });
            // Second item of category A (should NOT receive undefined)
            inputPipeline.add('item2', { category: 'A', value: 20 });
            // First item of category B (should receive undefined)
            inputPipeline.add('item3', { category: 'B', value: 30 });
            
            // Should have received undefined twice: once for A, once for B
            expect(undefinedCount.count).toBe(2);
        });
    });

    describe('edge cases', () => {
        it('should handle aggregates returning complex objects', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            
            type Stats = { min: number; max: number; sum: number; count: number };
            
            const addOp: AddOperator<ImmutableProps, Stats> = (acc, item) => {
                const value = (item as any).value;
                if (acc === undefined) {
                    return { min: value, max: value, sum: value, count: 1 };
                }
                return {
                    min: Math.min(acc.min, value),
                    max: Math.max(acc.max, value),
                    sum: acc.sum + value,
                    count: acc.count + 1
                };
            };
            const subtractOp: SubtractOperator<ImmutableProps, Stats> = (acc, item) => {
                const value = (item as any).value;
                return {
                    min: acc.min, // Note: can't properly update min on removal without full recalc
                    max: acc.max,
                    sum: acc.sum - value,
                    count: acc.count - 1
                };
            };
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'stats',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: Stats }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value: value as Stats });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            inputPipeline.add('item1', { category: 'A', value: 10 });
            inputPipeline.add('item2', { category: 'A', value: 30 });
            inputPipeline.add('item3', { category: 'A', value: 20 });
            
            const lastStats = modifiedEvents[modifiedEvents.length - 1].value;
            expect(lastStats.min).toBe(10);
            expect(lastStats.max).toBe(30);
            expect(lastStats.sum).toBe(60);
            expect(lastStats.count).toBe(3);
        });

        it('should handle single-element arrays', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).value;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).value;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'total',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value });
            });
            
            const inputPipeline = builder['input'] as { 
                add: (key: string, props: any) => void;
                remove: (key: string) => void;
            };
            
            inputPipeline.add('item1', { category: 'A', value: 42 });
            
            expect(modifiedEvents[0].value).toBe(42);
            
            inputPipeline.remove('item1');
            
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(0);
        });

        it('should handle zero values correctly', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).value;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).value;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'total',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            
            inputPipeline.add('item1', { category: 'A', value: 0 });
            inputPipeline.add('item2', { category: 'A', value: 100 });
            inputPipeline.add('item3', { category: 'A', value: 0 });
            
            expect(modifiedEvents[0].value).toBe(0);
            expect(modifiedEvents[1].value).toBe(100);
            expect(modifiedEvents[2].value).toBe(100);
        });

        it('should handle negative values correctly', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).value;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).value;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'total',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: any }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value });
            });
            
            const inputPipeline = builder['input'] as { 
                add: (key: string, props: any) => void;
                remove: (key: string) => void;
            };
            
            inputPipeline.add('item1', { category: 'A', value: 100 });
            inputPipeline.add('item2', { category: 'A', value: -30 });
            
            expect(modifiedEvents[0].value).toBe(100);
            expect(modifiedEvents[1].value).toBe(70);
            
            inputPipeline.remove('item2');
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(100);
        });
    });

    describe('event channel verification', () => {
        it('should ONLY emit aggregate through onModified, never through onAdded', () => {
            const builder = createPipeline<{ category: string; price: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'totalPrice',
                { add: addOp, subtract: subtractOp }
            );
            
            const addedProps: ImmutableProps[] = [];
            aggregateStep.onAdded([], (path, key, props) => {
                addedProps.push({ ...props });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            inputPipeline.add('item1', { category: 'Electronics', price: 500 });
            inputPipeline.add('item2', { category: 'Electronics', price: 300 });
            
            // Verify NO onAdded event contains the aggregate property
            addedProps.forEach(props => {
                expect(props).not.toHaveProperty('totalPrice');
            });
        });

        it('should emit aggregate property name correctly in onModified', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).value;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).value;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'myCustomAggregate',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedNames: string[] = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedNames.push(name);
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            inputPipeline.add('item1', { category: 'A', value: 10 });
            
            expect(modifiedNames).toContain('myCustomAggregate');
        });

        it('should maintain event separation with multiple aggregates (if chained)', () => {
            // This test verifies that adding multiple items still keeps the channels separate
            const builder = createPipeline<{ category: string; price: number; quantity: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).price;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).price;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'totalPrice',
                { add: addOp, subtract: subtractOp }
            );
            
            const addedEvents: Array<{ props: ImmutableProps }> = [];
            const modifiedEvents: Array<{ name: string; value: any }> = [];
            
            aggregateStep.onAdded([], (path, key, props) => {
                addedEvents.push({ props: { ...props } });
            });
            
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ name, value });
            });
            
            const inputPipeline = builder['input'] as { add: (key: string, props: any) => void };
            
            // Add 5 items
            for (let i = 0; i < 5; i++) {
                inputPipeline.add(`item${i}`, { category: 'A', price: 100, quantity: i + 1 });
            }
            
            // All addedEvents should NOT have aggregate
            addedEvents.forEach(e => {
                expect(e.props).toHaveProperty('category');
                expect(e.props).not.toHaveProperty('totalPrice');
            });
            
            // All modifiedEvents should have the aggregate property name
            modifiedEvents.forEach(e => {
                expect(e.name).toBe('totalPrice');
            });
            
            // Verify final aggregate value
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(500);
        });
    });

    describe('TypeDescriptor transformation', () => {
        it('should NOT remove target array from type descriptor (CommutativeAggregateStep)', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const inputDescriptor = step.getTypeDescriptor();
            
            // Input should have 'items' array
            expect(inputDescriptor.arrays.some(a => a.name === 'items')).toBe(true);
            
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).value;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).value;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'total',
                { add: addOp, subtract: subtractOp }
            );
            
            const outputDescriptor = aggregateStep.getTypeDescriptor();
            
            // CommutativeAggregateStep should NOT remove the array (that's DropArrayStep's job)
            expect(outputDescriptor.arrays.some(a => a.name === 'items')).toBe(true);
        });

        it('should remove target array from type descriptor when chained with DropArrayStep', () => {
            const builder = createPipeline<{ category: string; value: number }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const inputDescriptor = step.getTypeDescriptor();
            
            // Input should have 'items' array
            expect(inputDescriptor.arrays.some(a => a.name === 'items')).toBe(true);
            
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).value;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).value;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'total',
                { add: addOp, subtract: subtractOp }
            );
            
            // Chain with DropArrayStep to remove the array
            const dropArrayStep = new DropArrayStep(aggregateStep, ['items']);
            
            const outputDescriptor = dropArrayStep.getTypeDescriptor();
            
            // Output should NOT have 'items' array
            expect(outputDescriptor.arrays.some(a => a.name === 'items')).toBe(false);
        });

        it('should handle nested array path in type descriptor transformation with DropArrayStep', () => {
            const builder = createPipeline<{ state: string; city: string; venue: string; capacity: number }>()
                .groupBy(['state', 'city'], 'venues')
                .groupBy(['state'], 'cities');
            
            const step = builder['lastStep'] as Step;
            const inputDescriptor = step.getTypeDescriptor();
            
            // Input should have nested structure: cities > venues
            expect(inputDescriptor.arrays.some(a => a.name === 'cities')).toBe(true);
            const citiesArray = inputDescriptor.arrays.find(a => a.name === 'cities');
            expect(citiesArray?.type.arrays.some(a => a.name === 'venues')).toBe(true);
            
            const addOp: NumericAddOp = (acc, item) => (acc ?? 0) + (item as any).capacity;
            const subtractOp: NumericSubtractOp = (acc, item) => acc - (item as any).capacity;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['cities', 'venues'],
                'totalCapacity',
                { add: addOp, subtract: subtractOp }
            );
            
            // Chain with DropArrayStep to remove the nested array
            const dropArrayStep = new DropArrayStep(aggregateStep, ['cities', 'venues']);
            
            const outputDescriptor = dropArrayStep.getTypeDescriptor();
            
            // Output should still have 'cities' but without 'venues'
            expect(outputDescriptor.arrays.some(a => a.name === 'cities')).toBe(true);
            const outputCitiesArray = outputDescriptor.arrays.find(a => a.name === 'cities');
            expect(outputCitiesArray?.type.arrays.some(a => a.name === 'venues')).toBe(false);
        });
    });

    describe('count aggregation', () => {
        it('should correctly count items', () => {
            const builder = createPipeline<{ category: string; name: string }>()
                .groupBy(['category'], 'items');
            
            const step = builder['lastStep'] as Step;
            const addOp: NumericAddOp = (acc, _item) => (acc ?? 0) + 1;
            const subtractOp: NumericSubtractOp = (acc, _item) => acc - 1;
            
            const aggregateStep = new CommutativeAggregateStep(
                step,
                ['items'],
                'count',
                { add: addOp, subtract: subtractOp }
            );
            
            const modifiedEvents: Array<{ value: number }> = [];
            aggregateStep.onModified([], (path, key, name, value) => {
                modifiedEvents.push({ value });
            });
            
            const inputPipeline = builder['input'] as { 
                add: (key: string, props: any) => void;
                remove: (key: string) => void;
            };
            
            inputPipeline.add('item1', { category: 'A', name: 'First' });
            inputPipeline.add('item2', { category: 'A', name: 'Second' });
            inputPipeline.add('item3', { category: 'A', name: 'Third' });
            
            expect(modifiedEvents[0].value).toBe(1);
            expect(modifiedEvents[1].value).toBe(2);
            expect(modifiedEvents[2].value).toBe(3);
            
            inputPipeline.remove('item2');
            
            expect(modifiedEvents[modifiedEvents.length - 1].value).toBe(2);
        });
    });
});