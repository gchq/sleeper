import { useMemo } from 'react'
import { Link } from 'react-router-dom'
import Sparkline, { type SparklinePoint } from '../components/Sparkline'
import Title from '../components/Title'
import { useApi } from '../hooks/useApi'
import {
	METRIC_GROUPS,
	colorForMetric,
	formatByKind,
	type MetricDef,
	type MetricGroupDef,
} from '../lib/dataMetrics'
import './Data.css'

interface Comparison {
	current: number | null
	previous: number | null
}

interface RequestMetrics {
	head: Comparison
	get: Comparison
	put: Comparison
	post: Comparison
	delete: Comparison
}

interface ErrorMetrics {
	status4xx: Comparison
	status5xx: Comparison
}

interface DataMetrics {
	bucketName: string
	region: string
	consoleUrl: string
	bucketSizeBytes: Comparison
	numberOfObjects: Comparison
	requests: RequestMetrics
	bytesDownloaded: Comparison
	bytesUploaded: Comparison
	errors: ErrorMetrics
}

interface SeriesResponse {
	startTime: string
	endTime: string
	period: number
	series: Array<{ metric: string; points: Array<{ time: string; value: number | null }> }>
}

type ComparisonMap = Record<string, Comparison>
type SparklineMap = Record<string, SparklinePoint[]>

function collectComparisons(data: DataMetrics): ComparisonMap {
	return {
		bucketSizeBytes: data.bucketSizeBytes,
		numberOfObjects: data.numberOfObjects,
		head: data.requests.head,
		get: data.requests.get,
		put: data.requests.put,
		post: data.requests.post,
		delete: data.requests.delete,
		bytesDownloaded: data.bytesDownloaded,
		bytesUploaded: data.bytesUploaded,
		status4xx: data.errors.status4xx,
		status5xx: data.errors.status5xx,
	}
}

const STORAGE_METRIC_NAMES = ['BucketSizeBytes', 'NumberOfObjects']
const OPERATIONAL_METRIC_NAMES = [
	'HeadRequests', 'GetRequests', 'PutRequests', 'PostRequests', 'DeleteRequests',
	'BytesDownloaded', 'BytesUploaded',
	'4xxErrors', '5xxErrors',
]

function buildSeriesUrl(metricNames: string[], rangeSeconds: number, period: number, anchor: number): string {
	const endTime = new Date(anchor)
	const startTime = new Date(anchor - rangeSeconds * 1000)
	const params = new URLSearchParams()
	params.set('metrics', metricNames.join(','))
	params.set('startTime', startTime.toISOString())
	params.set('endTime', endTime.toISOString())
	params.set('period', String(period))
	return `/data/metrics?${params.toString()}`
}

function extractPoints(response: SeriesResponse | null, cloudWatchName: string): SparklinePoint[] {
	if (!response) return []
	const series = response.series.find((s) => s.metric === cloudWatchName)
	if (!series) return []
	return series.points
		.map((p) => ({ time: Date.parse(p.time), value: p.value }))
		.filter((p) => Number.isFinite(p.time))
}

interface DeltaOptions {
	format: (value: number | null) => string
	invertColor?: boolean
}

function Delta({ comparison, options }: { comparison: Comparison; options: DeltaOptions }) {
	const { current, previous } = comparison
	if (current === null || previous === null) {
		return <span className="metric-delta metric-delta-missing">no comparison</span>
	}
	const diff = current - previous
	if (diff === 0) {
		return <span className="metric-delta metric-delta-neutral">no change</span>
	}
	const positive = diff > 0
	const good = options.invertColor ? !positive : positive
	const cls = good ? 'metric-delta metric-delta-up' : 'metric-delta metric-delta-down'
	const arrow = positive ? '▲' : '▼'
	const magnitude = options.format(Math.abs(diff))
	const pct = previous === 0
		? null
		: ((diff / Math.abs(previous)) * 100)
	const pctText = pct === null || !Number.isFinite(pct) ? null : `${pct >= 0 ? '+' : ''}${pct.toFixed(1)}%`
	return (
		<span className={cls}>
			{arrow} {magnitude}
			{pctText && <span className="metric-delta-pct">({pctText})</span>}
		</span>
	)
}

interface MetricCardProps {
	group: MetricGroupDef
	metric: MetricDef
	comparison: Comparison
	compareLabel: string
	sparklinePoints: SparklinePoint[]
	sparklineDomain: [number, number]
	sparklineLoading: boolean
	sparklineWindow: string
}

function MetricCard({
	group,
	metric,
	comparison,
	compareLabel,
	sparklinePoints,
	sparklineDomain,
	sparklineLoading,
	sparklineWindow,
}: MetricCardProps) {
	const format = formatByKind(metric.format)
	const to = `/data/graph/${group.slug}?metrics=${encodeURIComponent(metric.key)}`
	const color = colorForMetric(group, metric)
	return (
		<Link className="metric-card metric-card-link" to={to} title={`View ${metric.label} graph`}>
			<div className="metric-label">{metric.label}</div>
			<div className="metric-value">{format(comparison.current)}</div>
			<div className="metric-compare">
				<Delta comparison={comparison} options={{ format, invertColor: metric.invertColor }} />
				<span className="metric-compare-label">{compareLabel}</span>
			</div>
			<div className="metric-sparkline">
				{sparklineLoading ? (
					<div className="sparkline sparkline-loading" aria-hidden="true" />
				) : (
					<Sparkline
						points={sparklinePoints}
						domain={sparklineDomain}
						color={color}
						height={36}
						title={`${metric.label} — ${sparklineWindow}`}
					/>
				)}
				<span className="metric-sparkline-label">{sparklineWindow}</span>
			</div>
		</Link>
	)
}

interface GroupSectionProps {
	group: MetricGroupDef
	comparisons: ComparisonMap
	compareLabel: string
	sparklines: SparklineMap
	sparklineDomain: [number, number]
	sparklineLoading: boolean
	sparklineWindow: string
}

function GroupSection({
	group,
	comparisons,
	compareLabel,
	sparklines,
	sparklineDomain,
	sparklineLoading,
	sparklineWindow,
}: GroupSectionProps) {
	return (
		<section className="data-section">
			<h3 className="data-section-title">{group.title}</h3>
			<div className="metric-grid">
				{group.metrics.map((metric) => (
					<MetricCard
						key={metric.key}
						group={group}
						metric={metric}
						comparison={comparisons[metric.key] ?? { current: null, previous: null }}
						compareLabel={compareLabel}
						sparklinePoints={sparklines[metric.key] ?? []}
						sparklineDomain={sparklineDomain}
						sparklineLoading={sparklineLoading}
						sparklineWindow={sparklineWindow}
					/>
				))}
			</div>
		</section>
	)
}

const DAY = 24 * 60 * 60
const HOUR = 60 * 60

export default function Data() {
	const { data, error, loading } = useApi<DataMetrics>('/data', 60, 30)

	// Round the anchor to the current minute so URLs stay stable across renders
	// (otherwise useApi would refetch every render).
	const anchor = useMemo(() => {
		const d = new Date()
		d.setSeconds(0, 0)
		return d.getTime()
	}, [])

	const storageUrl = useMemo(
		() => buildSeriesUrl(STORAGE_METRIC_NAMES, 7 * DAY, DAY, anchor),
		[anchor],
	)
	const operationalUrl = useMemo(
		() => buildSeriesUrl(OPERATIONAL_METRIC_NAMES, DAY, HOUR, anchor),
		[anchor],
	)

	const { data: storageData, loading: storageLoading } = useApi<SeriesResponse>(storageUrl, 0, 30)
	const { data: operationalData, loading: operationalLoading } = useApi<SeriesResponse>(operationalUrl, 0, 30)

	const sparklinesByGroup: Record<string, SparklineMap> = useMemo(() => {
		const map: Record<string, SparklineMap> = {}
		for (const group of METRIC_GROUPS) {
			const source = group.slug === 'storage' ? storageData : operationalData
			const entry: SparklineMap = {}
			for (const metric of group.metrics) {
				entry[metric.key] = extractPoints(source ?? null, metric.cloudWatchName)
			}
			map[group.slug] = entry
		}
		return map
	}, [storageData, operationalData])

	const storageDomain: [number, number] = [anchor - 7 * DAY * 1000, anchor]
	const operationalDomain: [number, number] = [anchor - DAY * 1000, anchor]

	if (error) {
		return (
			<>
				<Title>Data</Title>
				<div className="page">
					<h2>Data</h2>
					<p className="error">Failed to load data metrics: {error}</p>
				</div>
			</>
		)
	}

	if (!data) {
		return (
			<>
				<Title>Data</Title>
				<div className="page">
					<h2>Data</h2>
					<p>{loading ? 'Loading...' : 'No data available.'}</p>
				</div>
			</>
		)
	}

	const comparisons = collectComparisons(data)

	return (
		<>
			<Title>Data</Title>
			<div className="page">
				<h2>Data</h2>

				<section className="data-bucket-header">
					<div className="data-bucket-label">Data bucket</div>
					<div className="data-bucket-name">
						<code>{data.bucketName}</code>
						<a
							className="data-bucket-link"
							href={data.consoleUrl}
							target="_blank"
							rel="noopener noreferrer"
							title="Open in S3 console"
						>
							Open in S3 console ↗
						</a>
					</div>
				</section>

				{METRIC_GROUPS.map((group) => {
					const isStorage = group.slug === 'storage'
					return (
						<GroupSection
							key={group.slug}
							group={group}
							comparisons={comparisons}
							compareLabel={isStorage ? 'vs 24h ago' : 'vs previous 24h'}
							sparklines={sparklinesByGroup[group.slug] ?? {}}
							sparklineDomain={isStorage ? storageDomain : operationalDomain}
							sparklineLoading={isStorage ? storageLoading : operationalLoading}
							sparklineWindow={isStorage ? 'last 7 days' : 'last 24 hours'}
						/>
					)
				})}
			</div>
		</>
	)
}
