export type MetricFormatKind = 'bytes' | 'count'

export interface MetricDef {
	key: string
	label: string
	cloudWatchName: string
	format: MetricFormatKind
	invertColor?: boolean
}

export interface MetricGroupDef {
	slug: string
	title: string
	valueLabel: string
	stat: 'Sum' | 'Average'
	metrics: MetricDef[]
}

export const METRIC_GROUPS: MetricGroupDef[] = [
	{
		slug: 'storage',
		title: 'Storage',
		valueLabel: 'Average over period',
		stat: 'Average',
		metrics: [
			{ key: 'bucketSizeBytes', label: 'Bucket size', cloudWatchName: 'BucketSizeBytes', format: 'bytes' },
			{ key: 'numberOfObjects', label: 'Number of objects', cloudWatchName: 'NumberOfObjects', format: 'count' },
		],
	},
	{
		slug: 'requests',
		title: 'Requests',
		valueLabel: 'Sum per period',
		stat: 'Sum',
		metrics: [
			{ key: 'head', label: 'HEAD', cloudWatchName: 'HeadRequests', format: 'count' },
			{ key: 'get', label: 'GET', cloudWatchName: 'GetRequests', format: 'count' },
			{ key: 'put', label: 'PUT', cloudWatchName: 'PutRequests', format: 'count' },
			{ key: 'post', label: 'POST', cloudWatchName: 'PostRequests', format: 'count' },
			{ key: 'delete', label: 'DELETE', cloudWatchName: 'DeleteRequests', format: 'count' },
		],
	},
	{
		slug: 'transfer',
		title: 'Data transfer',
		valueLabel: 'Sum per period',
		stat: 'Sum',
		metrics: [
			{ key: 'bytesDownloaded', label: 'Bytes downloaded', cloudWatchName: 'BytesDownloaded', format: 'bytes' },
			{ key: 'bytesUploaded', label: 'Bytes uploaded', cloudWatchName: 'BytesUploaded', format: 'bytes' },
		],
	},
	{
		slug: 'errors',
		title: 'Errors',
		valueLabel: 'Sum per period',
		stat: 'Sum',
		metrics: [
			{ key: 'status4xx', label: '4xx errors', cloudWatchName: '4xxErrors', format: 'count', invertColor: true },
			{ key: 'status5xx', label: '5xx errors', cloudWatchName: '5xxErrors', format: 'count', invertColor: true },
		],
	},
]

export function findGroupBySlug(slug: string | undefined): MetricGroupDef | undefined {
	if (!slug) return undefined
	return METRIC_GROUPS.find((g) => g.slug === slug)
}

export function findGroupByMetricKey(key: string): MetricGroupDef | undefined {
	return METRIC_GROUPS.find((g) => g.metrics.some((m) => m.key === key))
}

export function formatBytes(bytes: number | null): string {
	if (bytes === null || bytes === undefined) return '—'
	if (!Number.isFinite(bytes)) return '—'
	if (bytes === 0) return '0 B'
	const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
	const i = Math.min(units.length - 1, Math.floor(Math.log(Math.abs(bytes)) / Math.log(1024)))
	const value = bytes / Math.pow(1024, i)
	const decimals = i === 0 || Math.abs(value) >= 100 ? 0 : Math.abs(value) >= 10 ? 1 : 2
	return `${value.toFixed(decimals)} ${units[i]}`
}

export function formatCount(value: number | null): string {
	if (value === null || value === undefined) return '—'
	if (!Number.isFinite(value)) return '—'
	return Math.round(value).toLocaleString()
}

export function formatCountCompact(value: number | null): string {
	if (value === null || value === undefined) return '—'
	if (!Number.isFinite(value)) return '—'
	const rounded = Math.round(value)
	const abs = Math.abs(rounded)
	if (abs < 1000) return rounded.toLocaleString()
	const units = [
		{ threshold: 1e12, suffix: 'T' },
		{ threshold: 1e9, suffix: 'B' },
		{ threshold: 1e6, suffix: 'M' },
		{ threshold: 1e3, suffix: 'K' },
	]
	const unit = units.find((u) => abs >= u.threshold)
	if (!unit) return rounded.toLocaleString()
	const scaled = rounded / unit.threshold
	const decimals = Math.abs(scaled) < 10 ? 1 : 0
	return `${scaled.toFixed(decimals).replace(/\.0$/, '')}${unit.suffix}`
}

export function formatByKind(kind: MetricFormatKind): (value: number | null) => string {
	return kind === 'bytes' ? formatBytes : formatCount
}

export const METRIC_COLORS = ['#1a56db', '#0f766e', '#b45309', '#7c3aed', '#be123c', '#0891b2', '#166534', '#c026d3']

export function colorForMetric(group: MetricGroupDef, metric: MetricDef): string {
	const idx = group.metrics.findIndex((m) => m.key === metric.key)
	return METRIC_COLORS[(idx < 0 ? 0 : idx) % METRIC_COLORS.length]
}
