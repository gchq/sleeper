
export function clampLimit(value: number, defaultLimit: number, maxLimit: number): number {
	if (!Number.isFinite(value) || value < 1) return defaultLimit
	return Math.min(value, maxLimit)
}