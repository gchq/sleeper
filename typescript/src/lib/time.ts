
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

// A TTL / retention period in seconds, rendered in whole days or hours where it divides evenly.
export function formatTtl(seconds: number): string {
	const days = seconds / 86400
	if (days >= 1 && Number.isInteger(days)) return `${days} day${days === 1 ? '' : 's'}`
	const hours = seconds / 3600
	if (hours >= 1 && Number.isInteger(hours)) return `${hours} hour${hours === 1 ? '' : 's'}`
	return `${seconds} seconds`
}

// Convert epoch millis to the value format expected by an <input type="datetime-local">
// (`YYYY-MM-DDTHH:mm`), interpreting the instant in the browser's local timezone.
export function epochToLocalInput(ms: number | null): string {
	if (ms == null || !Number.isFinite(ms)) return ''
	const d = new Date(ms)
	const pad = (n: number) => String(n).padStart(2, '0')
	return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
}

// Parse a datetime-local input value back to epoch millis, or null if empty/invalid.
export function localInputToEpoch(value: string): number | null {
	if (!value) return null
	const ms = new Date(value).getTime()
	return Number.isFinite(ms) ? ms : null
}