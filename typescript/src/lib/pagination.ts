
export function parseLimit(value: number, defaultLimit: number): number {
	if (!Number.isFinite(value) || value < 1) return defaultLimit
	return value
}