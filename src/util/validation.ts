export function validateArrayName(arrayName: string): void {
    if (arrayName.includes(':')) {
        throw new Error(`Array name cannot contain colon (:) character. Received: "${arrayName}"`);
    }
}

