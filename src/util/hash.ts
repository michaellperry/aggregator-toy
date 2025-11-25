import { encode as base64Encode } from '@stablelib/base64';
import { hash as sha512Hash } from '@stablelib/sha512';
import { encode as utf8Encode } from '@stablelib/utf8';

export function canonicalizeKeyProperties(obj: {}, keyProps: string[]): string {
    const sortedProps = [...keyProps].sort();
    const keyObject: Record<string, any> = {};
    for (const prop of sortedProps) {
        if (prop in obj) {
            keyObject[prop] = (obj as Record<string, any>)[prop];
        }
    }
    return JSON.stringify(keyObject);
}

function computeHash(str: string): string {
    const utf8Bytes = utf8Encode(str);
    const hashBytes = sha512Hash(utf8Bytes);
    return base64Encode(hashBytes);
}

export function computeKeyHash(obj: {}, keyProps: string[]): string {
    const canonical = canonicalizeKeyProperties(obj, keyProps);
    return computeHash(canonical);
}

