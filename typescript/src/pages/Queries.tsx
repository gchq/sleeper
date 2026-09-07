import type { ReactNode } from 'react'
import { useCallback, useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import Title from '../components/Title'
import ReloadButton from '../components/ReloadButton'
import QueryWizard from '../components/QueryWizard'
import QueryArchitecture from '../components/architectures/QueryArchitecture'
import { useApi } from '../hooks/useApi'
import { useSelectedTable } from '../hooks/useSelectedTable'
import { epochToLocalInput, formatEpochMillis, formatTtl, localInputToEpoch } from '../lib/time'
import {
	parseQueryStateFilter,
	queryStateBadgeClasses,
	queryStateFilterFor,
	queryStatusLabel,
	QUERY_STATES_IN_LIFECYCLE_ORDER,
	type QueryStateFilter,
} from '../lib/queryStatus'
import { parseLimit } from '../lib/pagination'
import { TableStatus } from '../contexts/InstanceContext'
import './Queries.css'

const DEFAULT_LIMIT = 100
const QUERY_ID_DEBOUNCE_MS = 400

export interface QuerySummary {
	queryId: string
	tableId: string | null
	tableName: string | null
	state: string
	lastUpdateTime: number | null
	expiryDate: number | null
	rowCount: number | null
	errorMessage: string | null
}

interface QueriesResponse {
	queries: QuerySummary[]
	limit: number
	hasMore: boolean
	queryTrackerTtlDays: number
}

export default function Queries() {
	const { table, features, loading } = useSelectedTable()

	if (loading && !features) {
		return (
			<Frame>
				<p className="queries-loading">Loading…</p>
			</Frame>
		)
	}

	if (!features?.Query) {
		return (
			<Frame>
				<div className="queries-unavailable">
					<p>The query stack isn't enabled for this instance.</p>
					<p>
						To enable it, add <code>QueryStack</code> to the <code>sleeper.optional.stacks</code> instance
						property, then redeploy the instance.
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

	return <QueriesContent table={table} />
}

function Frame({ children }: { children: ReactNode }) {
	return (
		<>
			<Title>Queries</Title>
			<div className="page">
				<h2>Queries</h2>
				{children}
			</div>
		</>
	)
}

function QueriesContent({ table }: { table?: TableStatus }) {
	const [searchParams, setSearchParams] = useSearchParams()
	const limit = parseLimit(Number(searchParams.get('limit') ?? DEFAULT_LIMIT), DEFAULT_LIMIT)
	const queryIdFilter = searchParams.get('queryId') ?? ''
	const state = parseQueryStateFilter(searchParams.get('state'))
	const fromMs = searchParams.has('from') ? Number(searchParams.get('from')) : null
	const toMs = searchParams.has('to') ? Number(searchParams.get('to')) : null

	const [queryIdInput, setQueryIdInput] = useState(queryIdFilter)
	const [queryOpen, setQueryOpen] = useState(false)

	const query = new URLSearchParams({ limit: String(limit) })
	if (table?.tableUniqueId) query.set('tableId', table.tableUniqueId)
	if (queryIdFilter) query.set('queryId', queryIdFilter)
	if (state !== 'all') query.set('state', state)
	if (fromMs != null) query.set('from', String(fromMs))
	if (toMs != null) query.set('to', String(toMs))

	const { data, loading, error, reload, nextReloadAt } = useApi<QueriesResponse>('/queries?' + query)
	const queries = data?.queries ?? []
	const hasMore = data?.hasMore ?? false
	const atMaxLimit = data != null && data.limit < limit

	// The query tracker ages entries off after its TTL, so a filter reaching further back than that
	// will silently return nothing for the expired part of the range.
	const ttlSeconds = data?.queryTrackerTtlDays ? data.queryTrackerTtlDays * 86400 : null
	const earliestRetainedMs = ttlSeconds ? Date.now() - ttlSeconds * 1000 : null
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
		if (queryIdInput === queryIdFilter) return
		const id = setTimeout(() => {
			setSearchParams(
				(prev) => {
					const next = new URLSearchParams(prev)
					if (queryIdInput.trim()) next.set('queryId', queryIdInput.trim())
					else next.delete('queryId')
					next.delete('limit')
					return next
				},
				{ replace: true },
			)
		}, QUERY_ID_DEBOUNCE_MS)
		return () => clearTimeout(id)
	}, [queryIdInput, queryIdFilter, setSearchParams])

	function changeState(next: QueryStateFilter) {
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
		setQueryIdInput('')
		setSearchParams(
			(prev) => {
				const params = new URLSearchParams(prev)
				for (const key of ['queryId', 'state', 'from', 'to', 'limit']) params.delete(key)
				return params
			},
			{ replace: true },
		)
	}

	const hasActiveFilters = queryIdFilter !== '' || state !== 'all' || fromMs != null || toMs != null

	const heading = table?.tableName ? `Queries: ${table.tableName}` : 'Queries'

	function queryLink(q: QuerySummary): string {
		const queryPath = encodeURIComponent(q.queryId)
		return `/queries/${queryPath}`
	}

	return (
		<>
			<Title>{heading}</Title>
			<div className="page">
				<div className="queries-header">
					<h2>{heading}</h2>
					<button type="button" className="btn btn-primary queries-query-btn" onClick={() => setQueryOpen(true)}>
						<svg
							aria-hidden="true"
							width="16"
							height="16"
							viewBox="0 0 16 16"
							fill="none"
							stroke="currentColor"
							strokeWidth="1.5"
							strokeLinecap="round"
							strokeLinejoin="round"
						>
							<circle cx="7" cy="7" r="4.5" />
							<path d="M10.5 10.5L14 14" />
						</svg>
						Query
					</button>
				</div>

				<QueryArchitecture />

				{ttlSeconds && timeRangeBeyondTtl && (
					<div className="queries-warning" role="alert">
						<p>
							The selected time range extends before {formatEpochMillis(earliestRetainedMs!)}, the earliest data retained
							by the query tracker (TTL {formatTtl(ttlSeconds)}). Queries older than this have expired and won't appear.
						</p>
						<Link
							className="btn queries-change-ttl"
							to={`/instance/properties?filter=${encodeURIComponent('sleeper.query.tracker.ttl.days')}&editable`}
						>
							Change TTL
						</Link>
					</div>
				)}

				{error && <p className="error">Failed to load queries: {error}</p>}

				<div className="queries-filters">
					<label className="queries-filter">
						<span>Query ID</span>
						<input
							type="search"
							placeholder="Exact query ID…"
							value={queryIdInput}
							onChange={(e) => setQueryIdInput(e.target.value)}
						/>
					</label>
					<label className="queries-filter">
						<span>From</span>
						<input
							type="datetime-local"
							value={epochToLocalInput(fromMs)}
							onChange={(e) => changeTime('from', e.target.value)}
						/>
					</label>
					<label className="queries-filter">
						<span>To</span>
						<input
							type="datetime-local"
							value={epochToLocalInput(toMs)}
							onChange={(e) => changeTime('to', e.target.value)}
						/>
					</label>
				</div>

				<div className="queries-controls">
					<div className="queries-state" role="group" aria-label="Query state filter">
						<button
							type="button"
							className={state === 'all' ? 'queries-state-btn active' : 'queries-state-btn'}
							onClick={() => changeState('all')}
						>
							All
						</button>
						{QUERY_STATES_IN_LIFECYCLE_ORDER.map((filterState) => (
							<button
								key={filterState}
								type="button"
								className={
									state === queryStateFilterFor(filterState) ? 'queries-state-btn active' : 'queries-state-btn'
								}
								onClick={() => changeState(queryStateFilterFor(filterState))}
							>
								{queryStatusLabel(filterState)}
							</button>
						))}
					</div>
					<div className="queries-actions">
						{hasActiveFilters && (
							<button type="button" className="btn queries-clear" onClick={clearFilters}>
								Clear filters
							</button>
						)}
						<ReloadButton onReload={reload} loading={loading} nextReloadAt={nextReloadAt} />
					</div>
				</div>

				{queryOpen && (
					<QueryWizard
						onClose={() => setQueryOpen(false)}
						onSubmitted={reload}
						presetTableId={table?.tableUniqueId}
					/>
				)}

				<table className="queries-table">
					<thead>
						<tr>
							<th>Last updated</th>
							<th>Query ID</th>
							{!table && <th>Table</th>}
							<th>State</th>
							<th>Rows</th>
						</tr>
					</thead>
					<tbody>
						{queries.map((q) => (
							<tr key={q.queryId}>
								<td>{formatEpochMillis(q.lastUpdateTime)}</td>
								<td className="queries-id">
									<Link to={queryLink(q)}>
										<code>{q.queryId}</code>
									</Link>
								</td>
								{!table && <td>{q.tableName ?? q.tableId ?? '—'}</td>}
								<td>
									<span className={queryStateBadgeClasses(q.state)}>{queryStatusLabel(q.state)}</span>
								</td>
								<td className="queries-num">{q.rowCount == null ? '—' : q.rowCount.toLocaleString()}</td>
							</tr>
						))}
					</tbody>
				</table>

				{loading && queries.length === 0 && <p className="queries-loading">Loading…</p>}
				{!loading && queries.length === 0 && !error && (
					<div className="queries-empty">
						<p>No queries{hasActiveFilters ? ' matching filters' : ''} found.</p>
						{hasActiveFilters && (
							<button type="button" className="btn" onClick={clearFilters}>
								Clear filters
							</button>
						)}
					</div>
				)}

				{hasMore && !loading && (
					<div className="queries-load-more">
						{atMaxLimit ? (
							<p className="queries-limit-reached">
								Showing the first {queries.length.toLocaleString()} queries, the most this page will load. Narrow
								the filters to see the rest.
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
