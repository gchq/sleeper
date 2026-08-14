import Title from '../components/Title'
import { useApi } from '../hooks/useApi'
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

function formatBytes(bytes: number | null): string {
	if (bytes === null || bytes === undefined) return '—'
	if (!Number.isFinite(bytes)) return '—'
	if (bytes === 0) return '0 B'
	const units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
	const i = Math.min(units.length - 1, Math.floor(Math.log(Math.abs(bytes)) / Math.log(1024)))
	const value = bytes / Math.pow(1024, i)
	const decimals = i === 0 || Math.abs(value) >= 100 ? 0 : Math.abs(value) >= 10 ? 1 : 2
	return `${value.toFixed(decimals)} ${units[i]}`
}

function formatCount(value: number | null): string {
	if (value === null || value === undefined) return '—'
	if (!Number.isFinite(value)) return '—'
	return Math.round(value).toLocaleString()
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
	label: string
	comparison: Comparison
	format: (value: number | null) => string
	invertColor?: boolean
	compareLabel?: string
}

function MetricCard({ label, comparison, format, invertColor, compareLabel = 'vs previous 24h' }: MetricCardProps) {
	return (
		<div className="metric-card">
			<div className="metric-label">{label}</div>
			<div className="metric-value">{format(comparison.current)}</div>
			<div className="metric-compare">
				<Delta comparison={comparison} options={{ format, invertColor }} />
				<span className="metric-compare-label">{compareLabel}</span>
			</div>
		</div>
	)
}

export default function Data() {
	const { data, error, loading } = useApi<DataMetrics>('/data', 60, 30)

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

				<section className="data-section">
					<h3 className="data-section-title">Storage</h3>
					<div className="metric-grid">
						<MetricCard
							label="Bucket size"
							comparison={data.bucketSizeBytes}
							format={formatBytes}
							compareLabel="vs 24h ago"
						/>
						<MetricCard
							label="Number of objects"
							comparison={data.numberOfObjects}
							format={formatCount}
							compareLabel="vs 24h ago"
						/>
					</div>
				</section>

				<section className="data-section">
					<h3 className="data-section-title">Requests (last 24h)</h3>
					<div className="metric-grid">
						<MetricCard label="HEAD" comparison={data.requests.head} format={formatCount} />
						<MetricCard label="GET" comparison={data.requests.get} format={formatCount} />
						<MetricCard label="PUT" comparison={data.requests.put} format={formatCount} />
						<MetricCard label="POST" comparison={data.requests.post} format={formatCount} />
						<MetricCard label="DELETE" comparison={data.requests.delete} format={formatCount} />
					</div>
				</section>

				<section className="data-section">
					<h3 className="data-section-title">Data transfer (last 24h)</h3>
					<div className="metric-grid">
						<MetricCard
							label="Bytes downloaded"
							comparison={data.bytesDownloaded}
							format={formatBytes}
						/>
						<MetricCard
							label="Bytes uploaded"
							comparison={data.bytesUploaded}
							format={formatBytes}
						/>
					</div>
				</section>

				<section className="data-section">
					<h3 className="data-section-title">Errors (last 24h)</h3>
					<div className="metric-grid">
						<MetricCard
							label="4xx errors"
							comparison={data.errors.status4xx}
							format={formatCount}
							invertColor
						/>
						<MetricCard
							label="5xx errors"
							comparison={data.errors.status5xx}
							format={formatCount}
							invertColor
						/>
					</div>
				</section>
			</div>
		</>
	)
}
