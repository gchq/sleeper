import { useMemo, type CSSProperties } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import {
	CartesianGrid,
	Legend,
	Line,
	LineChart,
	ResponsiveContainer,
	Tooltip,
	XAxis,
	YAxis,
} from 'recharts'
import Title from '../components/Title'
import { useApi } from '../hooks/useApi'
import {
	colorForMetric,
	findGroupBySlug,
	formatByKind,
	type MetricDef,
	type MetricGroupDef,
} from '../lib/dataMetrics'
import './DataMetricGraph.css'

interface Range {
	key: string
	label: string
	seconds: number
}

interface Period {
	key: string
	label: string
	seconds: number
}

const RANGES: Range[] = [
	{ key: '1h', label: 'Last 1 hour', seconds: 60 * 60 },
	{ key: '3h', label: 'Last 3 hours', seconds: 3 * 60 * 60 },
	{ key: '6h', label: 'Last 6 hours', seconds: 6 * 60 * 60 },
	{ key: '12h', label: 'Last 12 hours', seconds: 12 * 60 * 60 },
	{ key: '24h', label: 'Last 24 hours', seconds: 24 * 60 * 60 },
	{ key: '3d', label: 'Last 3 days', seconds: 3 * 24 * 60 * 60 },
	{ key: '7d', label: 'Last 7 days', seconds: 7 * 24 * 60 * 60 },
	{ key: '14d', label: 'Last 14 days', seconds: 14 * 24 * 60 * 60 },
	{ key: '30d', label: 'Last 30 days', seconds: 30 * 24 * 60 * 60 },
	{ key: '90d', label: 'Last 90 days', seconds: 90 * 24 * 60 * 60 },
	{ key: '1y', label: 'Last 1 year', seconds: 365 * 24 * 60 * 60 },
]

const PERIODS: Period[] = [
	{ key: '1m', label: '1 minute', seconds: 60 },
	{ key: '5m', label: '5 minutes', seconds: 5 * 60 },
	{ key: '15m', label: '15 minutes', seconds: 15 * 60 },
	{ key: '30m', label: '30 minutes', seconds: 30 * 60 },
	{ key: '1h', label: '1 hour', seconds: 60 * 60 },
	{ key: '3h', label: '3 hours', seconds: 3 * 60 * 60 },
	{ key: '6h', label: '6 hours', seconds: 6 * 60 * 60 },
	{ key: '12h', label: '12 hours', seconds: 12 * 60 * 60 },
	{ key: '1d', label: '1 day', seconds: 24 * 60 * 60 },
]

const MAX_POINTS_PER_SERIES = 2000
const DAY = 24 * 60 * 60

function periodsAllowedForRange(rangeSeconds: number): Period[] {
	return PERIODS.filter((p) => {
		const points = rangeSeconds / p.seconds
		if (points < 2) return false
		if (points > MAX_POINTS_PER_SERIES) return false
		// CloudWatch retention: 1-min data expires after 15 days;
		// sub-hour aggregations (5/15/30 min) are built from 5-min data, expires after 63 days.
		if (p.seconds === 60 && rangeSeconds > 15 * DAY) return false
		if (p.seconds < 60 * 60 && p.seconds !== 60 && rangeSeconds > 63 * DAY) return false
		return true
	})
}

function defaultPeriodForRange(rangeSeconds: number): Period {
	const allowed = periodsAllowedForRange(rangeSeconds)
	const desiredPoints = 120
	let best = allowed[0]
	let bestDist = Number.POSITIVE_INFINITY
	for (const p of allowed) {
		const dist = Math.abs(rangeSeconds / p.seconds - desiredPoints)
		if (dist < bestDist) {
			bestDist = dist
			best = p
		}
	}
	return best
}

interface Axis {
	id: string
	orientation: 'left' | 'right'
	format: (value: number | null) => string
	labels: string[]
}

function buildAxes(selected: MetricDef[]): { axes: Axis[]; axisIdForMetric: (m: MetricDef) => string } {
	const kinds: MetricDef['format'][] = []
	for (const m of selected) {
		if (!kinds.includes(m.format)) kinds.push(m.format)
	}
	const axes: Axis[] = kinds.map((kind, i) => ({
		id: kind,
		orientation: i === 0 ? 'left' : 'right',
		format: formatByKind(kind),
		labels: selected.filter((m) => m.format === kind).map((m) => m.label),
	}))
	return {
		axes,
		axisIdForMetric: (m: MetricDef) => m.format,
	}
}

interface BackendSeries {
	metric: string
	points: { time: string; value: number | null }[]
}

interface BackendResponse {
	startTime: string
	endTime: string
	period: number
	series: BackendSeries[]
}

function parseSelectedMetrics(param: string | null, group: MetricGroupDef): MetricDef[] {
	if (!param) return []
	const keys = new Set(param.split(',').map((s) => s.trim()).filter(Boolean))
	return group.metrics.filter((m) => keys.has(m.key))
}

function encodeMetrics(metrics: MetricDef[]): string {
	return metrics.map((m) => m.key).join(',')
}

function toChartData(response: BackendResponse, selected: MetricDef[]): Array<Record<string, number | null>> {
	if (!response.series.length) return []
	const timeIndex = new Map<number, Record<string, number | null>>()
	for (const s of response.series) {
		const def = selected.find((m) => m.cloudWatchName === s.metric)
		if (!def) continue
		for (const p of s.points) {
			const ts = Date.parse(p.time)
			if (!Number.isFinite(ts)) continue
			let row = timeIndex.get(ts)
			if (!row) {
				row = { time: ts }
				timeIndex.set(ts, row)
			}
			row[def.key] = p.value
		}
	}
	return Array.from(timeIndex.values()).sort((a, b) => (a.time as number) - (b.time as number))
}

function formatAxisTime(rangeSeconds: number): (t: number) => string {
	if (rangeSeconds <= 24 * 60 * 60) {
		return (t) => new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
	}
	if (rangeSeconds <= 7 * 24 * 60 * 60) {
		return (t) => new Date(t).toLocaleString([], { weekday: 'short', hour: '2-digit', minute: '2-digit' })
	}
	if (rangeSeconds <= 90 * 24 * 60 * 60) {
		return (t) => new Date(t).toLocaleDateString([], { month: 'short', day: 'numeric' })
	}
	return (t) => new Date(t).toLocaleDateString([], { month: 'short', year: '2-digit' })
}

function formatFullTime(t: number): string {
	return new Date(t).toLocaleString()
}

interface TooltipProps {
	active?: boolean
	payload?: Array<{ dataKey?: string; value?: number | null; color?: string; name?: string }>
	label?: number
	selected: MetricDef[]
}

function ChartTooltip({ active, payload, label, selected }: TooltipProps) {
	if (!active || !payload || !payload.length || label == null) return null
	return (
		<div className="graph-tooltip">
			<div className="graph-tooltip-time">{formatFullTime(label)}</div>
			<ul className="graph-tooltip-list">
				{payload.map((entry) => {
					const def = selected.find((m) => m.key === entry.dataKey)
					if (!def) return null
					const format = formatByKind(def.format)
					const val = entry.value === null || entry.value === undefined
						? '—'
						: format(entry.value)
					return (
						<li key={def.key}>
							<span className="graph-tooltip-swatch" style={{ background: entry.color }} />
							<span className="graph-tooltip-name">{def.label}</span>
							<span className="graph-tooltip-value">{val}</span>
						</li>
					)
				})}
			</ul>
		</div>
	)
}

export default function DataMetricGraph() {
	const { group: groupSlug } = useParams<{ group: string }>()
	const group = findGroupBySlug(groupSlug)

	if (!group) {
		return (
			<>
				<Title>Data graph</Title>
				<div className="page">
					<h2>Data graph</h2>
					<p className="error">Unknown metric group: {groupSlug}</p>
					<p><Link to="/data">← Back to Data</Link></p>
				</div>
			</>
		)
	}

	return <DataMetricGraphView group={group} />
}

function DataMetricGraphView({ group }: { group: MetricGroupDef }) {
	const [searchParams, setSearchParams] = useSearchParams()

	const rangeKey = searchParams.get('range') ?? '24h'
	const range = RANGES.find((r) => r.key === rangeKey) ?? RANGES[2]
	const allowedPeriods = periodsAllowedForRange(range.seconds)
	const requestedPeriodKey = searchParams.get('period')
	const period = allowedPeriods.find((p) => p.key === requestedPeriodKey) ?? defaultPeriodForRange(range.seconds)
	const metricsParam = searchParams.get('metrics')
	const selected = parseSelectedMetrics(metricsParam, group)
	const effectiveSelected = useMemo(() => {
		const parsed = parseSelectedMetrics(metricsParam, group)
		return parsed.length > 0 ? parsed : [group.metrics[0]]
	}, [metricsParam, group])
	const { axes, axisIdForMetric } = buildAxes(effectiveSelected)

	function updateParams(mutate: (params: URLSearchParams) => void) {
		const next = new URLSearchParams(searchParams)
		mutate(next)
		setSearchParams(next, { replace: true })
	}

	function toggleMetric(metric: MetricDef) {
		const isSelected = selected.some((m) => m.key === metric.key)
		let next: MetricDef[]
		if (isSelected) {
			next = selected.filter((m) => m.key !== metric.key)
		} else {
			next = [...selected, metric]
		}
		if (next.length === 0) return
		updateParams((p) => p.set('metrics', encodeMetrics(next)))
	}

	function changeRange(rangeKey: string) {
		const nextRange = RANGES.find((r) => r.key === rangeKey)
		if (!nextRange) return
		updateParams((p) => {
			p.set('range', nextRange.key)
			const nextAllowed = periodsAllowedForRange(nextRange.seconds)
			const existingPeriodKey = p.get('period')
			if (!existingPeriodKey || !nextAllowed.some((per) => per.key === existingPeriodKey)) {
				p.set('period', defaultPeriodForRange(nextRange.seconds).key)
			}
		})
	}

	function changePeriod(periodKey: string) {
		updateParams((p) => p.set('period', periodKey))
	}

	const timeWindow = useMemo(() => {
		const endTime = new Date()
		endTime.setSeconds(0, 0)
		const startTime = new Date(endTime.getTime() - range.seconds * 1000)
		return { start: startTime.getTime(), end: endTime.getTime() }
	}, [range.seconds])

	const requestUrl = useMemo(() => {
		const params = new URLSearchParams()
		params.set('metrics', effectiveSelected.map((m) => m.cloudWatchName).join(','))
		params.set('startTime', new Date(timeWindow.start).toISOString())
		params.set('endTime', new Date(timeWindow.end).toISOString())
		params.set('period', String(period.seconds))
		return `/data/metrics?${params.toString()}`
	}, [effectiveSelected, timeWindow.start, timeWindow.end, period.seconds])

	const { data, error, loading } = useApi<BackendResponse>(requestUrl, 0, 60)

	const chartData = useMemo(
		() => (data ? toChartData(data, effectiveSelected) : []),
		[data, effectiveSelected],
	)
	const xTickFormatter = useMemo(() => formatAxisTime(range.seconds), [range.seconds])

	return (
		<>
			<Title>{effectiveSelected.map((m) => m.label).join(', ')} - {group.title}</Title>
			<div className="page graph-page">
				<div className="graph-header">
					<Link to="/data" className="graph-back-link">← Back to Data</Link>
					<h2>{group.title}</h2>
				</div>

				<div className="graph-controls">
					<div className="graph-control">
						<span className="graph-control-label">Time range</span>
						<select value={range.key} onChange={(e) => changeRange(e.target.value)}>
							{RANGES.map((r) => (
								<option key={r.key} value={r.key}>{r.label}</option>
							))}
						</select>
					</div>
					<div className="graph-control">
						<span className="graph-control-label">Bin</span>
						<select value={period.key} onChange={(e) => changePeriod(e.target.value)}>
							{allowedPeriods.map((p) => (
								<option key={p.key} value={p.key}>{p.label}</option>
							))}
						</select>
					</div>
					<fieldset className="graph-metrics">
						<legend className="graph-control-label">Metrics</legend>
						{group.metrics.map((metric) => {
							const isSelected = effectiveSelected.some((m) => m.key === metric.key)
							const isOnlySelected = isSelected && effectiveSelected.length === 1
							const color = colorForMetric(group, metric)
							return (
								<label
									key={metric.key}
									className={isSelected ? 'graph-metric-toggle active' : 'graph-metric-toggle'}
									style={{ '--metric-color': color } as CSSProperties}
									title={isOnlySelected ? 'At least one metric must be selected' : ''}
								>
									<input
										type="checkbox"
										checked={isSelected}
										disabled={isOnlySelected}
										onChange={() => toggleMetric(metric)}
									/>
									<span className="graph-metric-swatch" aria-hidden="true" />
									{metric.label}
								</label>
							)
						})}
					</fieldset>
				</div>

				{error && <p className="error">Failed to load metrics: {error}</p>}
				{!data && !error && <p>{loading ? 'Loading...' : 'No data.'}</p>}

				{data && chartData.length > 0 && (
					<div className="graph-container">
						<ResponsiveContainer width="100%" height={480}>
							<LineChart data={chartData} margin={{ top: 12, right: 24, bottom: 12, left: 12 }}>
								<CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
								<XAxis
									dataKey="time"
									type="number"
									scale="time"
									domain={[timeWindow.start, timeWindow.end]}
									tickFormatter={xTickFormatter}
									minTickGap={40}
									allowDataOverflow
								/>
								{axes.map((axis) => (
									<YAxis
										key={axis.id}
										yAxisId={axis.id}
										orientation={axis.orientation}
										tickFormatter={(v: number) => axis.format(v)}
										width={axis.orientation === 'left' ? 110 : 80}
										label={
											axes.length > 1
												? {
													value: axis.labels.join(', '),
													angle: axis.orientation === 'left' ? -90 : 90,
													position: axis.orientation === 'left' ? 'insideLeft' : 'insideRight',
													offset: 0,
													style: { textAnchor: 'middle', fill: '#4b5563', fontSize: 14, fontWeight: 500 },
												}
												: undefined
										}
									/>
								))}
								<Tooltip
									content={
										<ChartTooltip selected={effectiveSelected} />
									}
								/>
								<Legend />
								{effectiveSelected.map((metric) => (
									<Line
										key={metric.key}
										yAxisId={axisIdForMetric(metric)}
										type="monotone"
										dataKey={metric.key}
										name={metric.label}
										stroke={colorForMetric(group, metric)}
										dot={false}
										strokeWidth={2}
										connectNulls
										isAnimationActive={false}
									/>
								))}
							</LineChart>
						</ResponsiveContainer>
					</div>
				)}
				{data && chartData.length === 0 && !error && (
					<p className="graph-empty">No data points returned by CloudWatch for this range.</p>
				)}
			</div>
		</>
	)
}
