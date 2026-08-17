import { useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import Sparkline, { type SparklinePoint } from '../components/Sparkline'
import Title from '../components/Title'
import { useApi } from '../hooks/useApi'
import { useTablesList } from '../contexts/TablesContext'
import { formatCount, METRIC_COLORS } from '../lib/dataMetrics'
import CreateTableWizard from './CreateTableWizard'
import './Tables.css'

interface RowCountPoint {
	time: string
	value: number | null
}

interface TableRowCounts {
	tableUniqueId: string
	tableName: string
	latestRowCount: number | null
	points: RowCountPoint[]
}

interface RowCountsResponse {
	startTime: string
	endTime: string
	period: number
	series: TableRowCounts[]
}

const ROW_COUNT_COLOR = METRIC_COLORS[1]

function toSparklinePoints(points: RowCountPoint[]): SparklinePoint[] {
	return points
		.map((p) => ({ time: Date.parse(p.time), value: p.value }))
		.filter((p) => Number.isFinite(p.time))
}

function RowCountCell({
	value,
	points,
	domain,
	loading,
}: {
	value: number | null
	points: SparklinePoint[]
	domain: [number, number]
	loading: boolean
}) {
	return (
		<div className="tables-rows">
			<span className="tables-rows-value">{formatCount(value)}</span>
			{loading ? (
				<div className="sparkline sparkline-loading" aria-hidden="true" />
			) : (
				<Sparkline
					points={points}
					domain={domain}
					color={ROW_COUNT_COLOR}
					width={110}
					height={30}
					title="Row count — last 7 days"
				/>
			)}
		</div>
	)
}

export default function Tables() {
	const { tables, loading, error, reload } = useTablesList()
	const [wizardOpen, setWizardOpen] = useState(false)
	const { data: rowCounts, loading: rowCountsLoading } = useApi<RowCountsResponse>('/tables/row-counts', 60, 30)

	const pointsByTableId = useMemo(() => {
		const map = new Map<string, SparklinePoint[]>()
		if (rowCounts) {
			for (const s of rowCounts.series) {
				map.set(s.tableUniqueId, toSparklinePoints(s.points))
			}
		}
		return map
	}, [rowCounts])

	const latestByTableId = useMemo(() => {
		const map = new Map<string, number | null>()
		if (rowCounts) {
			for (const s of rowCounts.series) {
				map.set(s.tableUniqueId, s.latestRowCount)
			}
		}
		return map
	}, [rowCounts])

	const domain: [number, number] = useMemo(() => {
		if (rowCounts) {
			const start = Date.parse(rowCounts.startTime)
			const end = Date.parse(rowCounts.endTime)
			if (Number.isFinite(start) && Number.isFinite(end)) return [start, end]
		}
		const now = Date.now()
		return [now - 7 * 24 * 60 * 60 * 1000, now]
	}, [rowCounts])

	return (
		<>
			<Title>Tables</Title>
			<div className="page">
				<div className="tables-header">
					<h2>Tables</h2>
					<button className="btn btn-primary" onClick={() => setWizardOpen(true)}>
						Create table
					</button>
				</div>

				{error && <p className="error">Failed to load tables: {error}</p>}

				{!error && !tables && <p>{loading ? 'Loading...' : 'No data.'}</p>}

				{!error && tables && tables.length === 0 && (
					<p className="tables-empty">
						No tables yet. Click <strong>Create table</strong> to add one.
					</p>
				)}

				{!error && tables && tables.length > 0 && (
					<table className="tables-table">
						<thead>
							<tr>
								<th>Name</th>
								<th>Table ID</th>
								<th>Rows (last 7 days)</th>
								<th>Status</th>
								<th />
							</tr>
						</thead>
						<tbody>
							{tables.map((t) => (
								<tr key={t.tableUniqueId}>
									<td className="tables-name">{t.tableName}</td>
									<td className="tables-id">
										<code>{t.tableUniqueId}</code>
									</td>
									<td>
										<RowCountCell
											value={latestByTableId.get(t.tableUniqueId) ?? null}
											points={pointsByTableId.get(t.tableUniqueId) ?? []}
											domain={domain}
											loading={rowCountsLoading && !rowCounts}
										/>
									</td>
									<td>
										<span className={t.online ? 'tables-status online' : 'tables-status offline'}>
											{t.online ? 'Online' : 'Offline'}
										</span>
									</td>
									<td className="tables-actions">
										<Link
											to={`/tables/${encodeURIComponent(t.tableUniqueId)}/properties`}
											className="btn-link"
										>
											Properties
										</Link>
									</td>
								</tr>
							))}
						</tbody>
					</table>
				)}
			</div>

			{wizardOpen && (
				<CreateTableWizard
					onClose={() => setWizardOpen(false)}
					onCreated={() => reload()}
				/>
			)}
		</>
	)
}
