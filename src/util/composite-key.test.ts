import { createCompositeKey, parseCompositeKey } from './composite-key';

describe('composite-key utilities', () => {
    describe('createCompositeKey', () => {
        it('should create correct format: groupKey:arrayName:itemKey', () => {
            const result = createCompositeKey('group123', 'items', 'item456');
            expect(result).toBe('group123:items:item456');
        });

        it('should handle different key formats', () => {
            expect(createCompositeKey('abc', 'items', 'xyz')).toBe('abc:items:xyz');
            expect(createCompositeKey('', 'items', 'item')).toBe(':items:item');
        });
    });

    describe('parseCompositeKey', () => {
        it('should correctly parse valid composite keys', () => {
            const result = parseCompositeKey('group123:items:item456');
            expect(result).toEqual({
                groupKey: 'group123',
                arrayName: 'items',
                itemKey: 'item456'
            });
        });

        it('should return null for non-composite keys', () => {
            expect(parseCompositeKey('simpleKey')).toBeNull();
            expect(parseCompositeKey('group:item')).toBeNull(); // Only one colon
            expect(parseCompositeKey('')).toBeNull();
        });

        it('should handle edge cases', () => {
            // Empty strings
            expect(parseCompositeKey(':items:')).toEqual({
                groupKey: '',
                arrayName: 'items',
                itemKey: ''
            });

            // Item key can contain colons (groupKey and arrayName don't)
            const result = parseCompositeKey('groupKey:items:item:with:colons');
            expect(result).toEqual({
                groupKey: 'groupKey',
                arrayName: 'items',
                itemKey: 'item:with:colons'
            });
        });
    });
});

