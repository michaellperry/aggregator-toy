/**
 * Checks if two path arrays match exactly.
 * 
 * @param pathNames - The path to check
 * @param targetPath - The target path to compare against
 * @returns true if the paths match exactly
 */
export function pathsMatch(pathNames: string[], targetPath: string[]): boolean {
    if (pathNames.length !== targetPath.length) {
        return false;
    }
    return pathNames.every((name, i) => name === targetPath[i]);
}

/**
 * Checks if pathNames starts with the given prefix path.
 * 
 * @param pathNames - The path to check
 * @param prefixPath - The prefix path to compare against
 * @returns true if pathNames starts with prefixPath
 */
export function pathStartsWith(pathNames: string[], prefixPath: string[]): boolean {
    if (pathNames.length < prefixPath.length) {
        return false;
    }
    return prefixPath.every((name, i) => name === pathNames[i]);
}