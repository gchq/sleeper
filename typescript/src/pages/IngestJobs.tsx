import type { ReactNode } from 'react'
import { useCallback, useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import Title from '../components/Title'
import ReloadButton from '../components/ReloadButton'
import { useApi } from '../hooks/useApi'
import { useSelectedTable } from '../hooks/useSelectedTable'
import { epochToLocalInput, formatTimestamp, formatTtl, localInputToEpoch } from '../lib/time'
import { statusClass, statusLabel } from '../lib/ingestStatus'
import { parseLimit } from '../lib/pagination'
import { TableStatus } from '../contexts/InstanceContext'
import './IngestJobs.css'
import IngestTrackingArchitecture from '../components/architectures/IngestTrackingArchitecture'

const DEFAULT_LIMIT = 100
const JOB_ID_DEBOUNCE_MS = 400

type StateFilter = 'all' | 'rejected' | 'failed' | 'running' | 'finished'

const STATE_FILTERS: { value: StateFilter; label: string }[] = [
	{ value: 'all', label: 'All' },
	{ value: 'rejected', label: 'Rejected' },
	{ value: 'running', label: 'Running' },
	{ value: 'finished', label: 'Finished' },
	{ value: 'failed', label: 'Failed' },
]

export interface IngestJobSummary {
	jobId: string
	tableId: string | null
	tableName: string | null
	status: string
	inputFileCount: number
	runCount: number
	startTime: string | null
	finishTime: string | null
	rowsWritten: number | null
}

interface IngestJobsResponse {
	jobs: IngestJobSummary[]
	limit: number
	hasMore: boolean
	jobStatusTtlSeconds: number
}

function parseState(value: string | null): StateFilter {
	switch (value) {
		case 'rejected':
		case 'failed':
		case 'running':
		case 'finished':
			return value
		default:
			return 'all'
	}
}

export default function IngestJobs() {
	const { table, features, loading } = useSelectedTable()

	if (loading && !features) {
		return (
			<Frame>
				<p className="jobs-loading">Loading…</p>
			</Frame>
		)
	}

	if (!features?.IngestTracking) {
		return (
			<Frame>
				<div className="jobs-unavailable">
					<p>Ingest tracking isn't enabled for this instance.</p>
					<p>
						To enable it, set the <code>sleeper.ingest.tracker.enabled</code> instance property to{' '}
						<code>true</code>, then redeploy the instance.
					</p>
				</div>
			</Frame>
		)
	}

	if (table === null) {
		return (
			<Frame>
				<p className="error">Table not found.</p>
			</Frame>
		)
	}

	return <IngestJobsContent table={table} />
}

function Frame({ children }: { children: ReactNode }) {
	return (
		<>
			<Title>Ingest Jobs</Title>
			<div className="page">
				<h2>Ingest Jobs</h2>
				{children}
			</div>
		</>
	)
}

function IngestJobsContent({ table }: { table?: TableStatus }) {
	const [searchParams, setSearchParams] = useSearchParams()
	const limit = parseLimit(Number(searchParams.get('limit') ?? DEFAULT_LIMIT), DEFAULT_LIMIT)
	const jobIdFilter = searchParams.get('jobId') ?? ''
	const state = parseState(searchParams.get('state'))
	const fromMs = searchParams.has('from') ? Number(searchParams.get('from')) : null
	const toMs = searchParams.has('to') ? Number(searchParams.get('to')) : null

	const [jobIdInput, setJobIdInput] = useState(jobIdFilter)

	const query = new URLSearchParams({ limit: String(limit) })
	if (table?.tableUniqueId) query.set('tableId', table.tableUniqueId)
	if (jobIdFilter) query.set('jobId', jobIdFilter)
	if (state !== 'all') query.set('state', state)
	if (fromMs != null) query.set('from', String(fromMs))
	if (toMs != null) query.set('to', String(toMs))

	const { data, loading, error, reload, nextReloadAt } = useApi<IngestJobsResponse>('/ingest-tracking/jobs?' + query)
	const jobs = data?.jobs ?? []
	const hasMore = data?.hasMore ?? false
	// The API caps the page size. When it hands back a smaller limit than we asked for, we've hit
	// that cap and asking for more would return the same page again.
	const atMaxLimit = data != null && data.limit < limit

	const earliestRetainedMs = data?.jobStatusTtlSeconds ? Date.now() - data.jobStatusTtlSeconds * 1000 : null
	const timeRangeBeyondTtl = earliestRetainedMs != null && ((fromMs != null && fromMs < earliestRetainedMs) || (toMs != null && toMs < earliestRetainedMs))

	const loadMore = useCallback(() => {
		setSearchParams(
			(prev) => {
				const next = new URLSearchParams(prev)
				const current = parseLimit(Number(next.get('limit') ?? DEFAULT_LIMIT), DEFAULT_LIMIT)
				next.set('limit', String(current + DEFAULT_LIMIT))
				return next
			},
			{ replace: true },
		)
	}, [setSearchParams])

	useEffect(() => {
		if (jobIdInput === jobIdFilter) return
		const id = setTimeout(() => {
			setSearchParams(
				(prev) => {
					const next = new URLSearchParams(prev)
					if (jobIdInput.trim()) next.set('jobId', jobIdInput.trim())
					else next.delete('jobId')
					next.delete('limit')
					return next
				},
				{ replace: true },
			)
		}, JOB_ID_DEBOUNCE_MS)
		return () => clearTimeout(id)
	}, [jobIdInput, jobIdFilter, setSearchParams])

	function changeState(next: StateFilter) {
		if (next === state) return
		setSearchParams((prev) => {
			const params = new URLSearchParams(prev)
			if (next === 'all') params.delete('state')
			else params.set('state', next)
			params.delete('limit')
			return params
		})
	}

	function changeTime(key: 'from' | 'to', value: string) {
		const ms = localInputToEpoch(value)
		setSearchParams((prev) => {
			const params = new URLSearchParams(prev)
			if (ms == null) params.delete(key)
			else params.set(key, String(ms))
			params.delete('limit')
			return params
		})
	}

	function clearFilters() {
		setJobIdInput('')
		setSearchParams(
			(prev) => {
				const params = new URLSearchParams(prev)
				for (const key of ['jobId', 'state', 'from', 'to', 'limit']) params.delete(key)
				return params
			},
			{ replace: true },
		)
	}

	const hasActiveFilters = jobIdFilter !== '' || state !== 'all' || fromMs != null || toMs != null

	const heading = table?.tableName ? `Ingest Jobs: ${table.tableName}` : 'Ingest Jobs'

	function jobLink(job: IngestJobSummary): string {
		const jobPath = encodeURIComponent(job.jobId)
		const tableId = job.tableId ?? table?.tableUniqueId ?? undefined
		return tableId
			? `/tables/${encodeURIComponent(tableId)}/ingest-jobs/${jobPath}`
			: `/ingest-jobs/${jobPath}`
	}

	return (
		<>
			<Title>{heading}</Title>
			<div className="page">
				<h2>{heading}</h2>

				<IngestTrackingArchitecture />

				{data?.jobStatusTtlSeconds && timeRangeBeyondTtl && (
					<div className="jobs-warning" role="alert">
						<p>
							The selected time range extends before {formatTimestamp(new Date(earliestRetainedMs!).toISOString())},
							the earliest data retained by the job tracker (TTL {formatTtl(data.jobStatusTtlSeconds)}). Jobs older than
							this have expired and won't appear.
						</p>
						<Link
							className="btn jobs-change-ttl"
							to={`/instance/properties?filter=${encodeURIComponent('sleeper.ingest.*.status.ttl')}&editable`}
						>
							Change TTL
						</Link>
					</div>
				)}

				{error && <p className="error">Failed to load ingest jobs: {error}</p>}

				<div className="jobs-filters">
					<label className="jobs-filter">
						<span>Job ID</span>
						<input
							type="search"
							placeholder="Exact job ID…"
							value={jobIdInput}
							onChange={(e) => setJobIdInput(e.target.value)}
						/>
					</label>
					<label className="jobs-filter">
						<span>Active from</span>
						<input
							type="datetime-local"
							value={epochToLocalInput(fromMs)}
							onChange={(e) => changeTime('from', e.target.value)}
						/>
					</label>
					<label className="jobs-filter">
						<span>Active to</span>
						<input
							type="datetime-local"
							value={epochToLocalInput(toMs)}
							onChange={(e) => changeTime('to', e.target.value)}
						/>
					</label>
					{hasActiveFilters && (
						<button type="button" className="btn jobs-clear" onClick={clearFilters}>
							Clear filters
						</button>
					)}
				</div>

				<div className="jobs-controls">
					<div className="jobs-state" role="group" aria-label="Job state filter">
						{STATE_FILTERS.map((f) => (
							<button
								key={f.value}
								type="button"
								className={state === f.value ? 'jobs-state-btn active' : 'jobs-state-btn'}
								onClick={() => changeState(f.value)}
							>
								{f.label}
							</button>
						))}
					</div>
					<ReloadButton onReload={reload} loading={loading} nextReloadAt={nextReloadAt} />
				</div>

				<table className="jobs-table">
					<thead>
						<tr>
							<th>Started</th>
							<th>Job ID</th>
							{!table && <th>Table</th>}
							<th>Status</th>
							<th>Files</th>
							<th>Rows written</th>
						</tr>
					</thead>
					<tbody>
						{jobs.map((job) => (
							<tr key={job.jobId}>
								<td>{job.startTime ? formatTimestamp(job.startTime) : '—'}</td>
								<td className="jobs-id">
									<Link to={jobLink(job)}>
										<code>{job.jobId}</code>
									</Link>
								</td>
								{!table && <td>{job.tableName ?? job.tableId ?? '—'}</td>}
								<td>
									<span className={statusClass(job.status)}>{statusLabel(job.status)}</span>
								</td>
								<td className="jobs-num">{job.inputFileCount.toLocaleString()}</td>
								<td className="jobs-num">
									{job.rowsWritten == null ? '—' : job.rowsWritten.toLocaleString()}
								</td>
							</tr>
						))}
					</tbody>
				</table>

				{loading && jobs.length === 0 && <p className="jobs-loading">Loading…</p>}
				{!loading && jobs.length === 0 && !error && (
					<div className="jobs-empty">
						<p>No ingest jobs{hasActiveFilters ? ' matching filters' : ''} found.</p>
						{hasActiveFilters && (
							<button type="button" className="btn" onClick={clearFilters}>
								Clear filters
							</button>
						)}
					</div>
				)}

				{hasMore && !loading && (
					<div className="jobs-load-more">
						{atMaxLimit ? (
							<p className="jobs-limit-reached">
								Showing the first {jobs.length.toLocaleString()} jobs, the most this page will load. Narrow the
								filters to see the rest.
							</p>
						) : (
							<button type="button" className="btn" onClick={loadMore}>
								Load more
							</button>
						)}
					</div>
				)}
			</div>
		</>
	)
}
