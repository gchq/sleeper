
export function formatTimestamp(iso: string): string {
	const ms = Date.parse(iso)
	if (!Number.isFinite(ms)) return iso
	return new Date(ms).toLocaleString()
}

export function formatDurationSeconds(seconds: number | string): string {
	const value = typeof seconds === 'number' ? seconds : Number(seconds)
	if (!Number.isFinite(value)) return String(seconds)
	if (value < 60) return `${value} second${value === 1 ? '' : 's'}`
	if (value < 3600) {
		const mins = Math.round(value / 60)
		return `${mins} minute${mins === 1 ? '' : 's'}`
	}
	const hours = Math.round((value / 3600) * 10) / 10
	return `${hours} hour${hours === 1 ? '' : 's'}`
}