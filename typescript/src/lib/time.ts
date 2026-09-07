
export function formatTimestamp(iso: string): string {
	const ms = Date.parse(iso)
	if (!Number.isFinite(ms)) return iso
	return new Date(ms).toLocaleString()
}

export function formatEpochMillis(ms: number | null | undefined): string {
	if (ms == null || !Number.isFinite(ms)) return '—'
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

export function formatDurationMillisSpan(ms: number | null | undefined): string {
	if (ms == null || !Number.isFinite(ms) || ms < 0) return '—'
	const totalSeconds = Math.floor(ms / 1000)
	if (totalSeconds < 1) return '<1s'
	if (totalSeconds < 60) return `${totalSeconds}s`
	const minutes = Math.floor(totalSeconds / 60)
	const seconds = totalSeconds % 60
	if (minutes < 60) return `${minutes}m ${seconds}s`
	const hours = Math.floor(minutes / 60)
	return `${hours}h ${minutes % 60}m`
}

export function durationBetween(start: number | null | undefined, end: number | null | undefined): number | null {
	if (start == null || end == null || !Number.isFinite(start) || !Number.isFinite(end)) return null
	return end - start
}

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