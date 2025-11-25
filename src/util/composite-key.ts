export function createCompositeKey(groupKey: string, arrayName: string, itemKey: string): string {
    return `${groupKey}:${arrayName}:${itemKey}`;
}

export function parseCompositeKey(compositeKey: string): { groupKey: string, arrayName: string, itemKey: string } | null {
    const parts = compositeKey.split(':');
    
    // Must have exactly 3 parts (groupKey:arrayName:itemKey)
    // But groupKey and itemKey can contain colons, so we need to split differently
    // Format is: groupKey:arrayName:itemKey
    // We know arrayName doesn't contain colons (validated), so we can find it
    
    // Find the first colon (end of groupKey)
    const firstColonIndex = compositeKey.indexOf(':');
    if (firstColonIndex === -1) {
        return null; // No colon found
    }
    
    // Find the second colon (end of arrayName)
    const secondColonIndex = compositeKey.indexOf(':', firstColonIndex + 1);
    if (secondColonIndex === -1) {
        return null; // Only one colon found
    }
    
    const groupKey = compositeKey.substring(0, firstColonIndex);
    const arrayName = compositeKey.substring(firstColonIndex + 1, secondColonIndex);
    const itemKey = compositeKey.substring(secondColonIndex + 1);
    
    return { groupKey, arrayName, itemKey };
}

