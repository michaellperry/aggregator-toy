import { validateArrayName } from './validation';

describe('validateArrayName', () => {
    it('should accept valid names (no colons)', () => {
        expect(() => validateArrayName('items')).not.toThrow();
        expect(() => validateArrayName('subItems')).not.toThrow();
        expect(() => validateArrayName('myArray')).not.toThrow();
        expect(() => validateArrayName('items123')).not.toThrow();
    });

    it('should throw error for names containing colon', () => {
        expect(() => validateArrayName('items:sub')).toThrow();
        expect(() => validateArrayName('items:')).toThrow();
        expect(() => validateArrayName(':items')).toThrow();
        expect(() => validateArrayName('it:ems')).toThrow();
    });

    it('should throw error for names starting/ending with colon', () => {
        expect(() => validateArrayName(':items')).toThrow();
        expect(() => validateArrayName('items:')).toThrow();
        expect(() => validateArrayName(':')).toThrow();
    });
});

