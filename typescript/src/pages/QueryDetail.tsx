import type { ReactNode } from 'react'
import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import Title from '../components/Title'
import ReloadButton from '../components/ReloadButton'
import QueryDestination from '../components/QueryDestination'
import TrackedQuerySummary from '../components/TrackedQuerySummary'
import { useApi } from '../hooks/useApi'
import { useNow } from '../hooks/useNow'
import { useSelectedTable } from '../hooks/useSelectedTable'
import { formatEpochMillis, formatDurationMillisSpan, durationBetween } from '../lib/time'
import {
	isQueryFinished,
	parseQueryState,
	queryStateBadgeClasses,
	queryStateModifierClass,
	queryStatusLabel,
	QUERY_STATES,
	type QueryState,
} from '../lib/queryStatus'
import { formatNum, formatRowCount, type QueryDetailData } from '../lib/queryResults'
import './QueryDetail.css'

export default function QueryDetail() {
	const { queryId } = useParams<{ queryId: string }>()
	const { features, loading } = useSelectedTable()
	if (!queryId) return <></>

	if (loading && !features) {
		return (
			<Frame>
				<p className="query-detail-loading">Loading…</p>
			</Frame>
		)
	}

	if (!features?.Query) {
		return (
			<Frame>
				<div className="queries-unavailable">
					<p>The query stack isn't enabled for this instance.</p>
				</div>
			</Frame>
		)
	}

	return <QueryDetailContent queryId={queryId} />
}

function Frame({ children }: { children: ReactNode }) {
	return (
		<>
			<Title>Query</Title>
			<div className="page">
				<h2>Query</h2>
				{children}
			</div>
		</>
	)
}

export const ACTIVE_REFRESH_SECONDS = 5
export const IDLE_REFRESH_SECONDS = 60
const NOT_FOUND_GRACE_SECONDS = 30

function QueryDetailContent({ queryId }: { queryId: string }) {
	const [stateFilter, setStateFilter] = useState<QueryState | null>(null)
	const [refreshInterval, setRefreshInterval] = useState(ACTIVE_REFRESH_SECONDS)
	const { data, loading, error, errorStatus, reload, nextReloadAt } = useApi<QueryDetailData>(
		'/query/' + encodeURIComponent(queryId),
		refreshInterval,
	)

	// A freshly submitted query isn't in the tracker until the first lambda picks it off the queue,
	// so the API returns a 404 for a short while. Remember when the run of 404s started, to tell that
	// case apart from a query ID that will never show up.
	const notFound = errorStatus === 404
	const [notFoundSince, setNotFoundSince] = useState<number | null>(null)
	useEffect(() => {
		setNotFoundSince(notFound ? Date.now() : null)
	}, [notFound])

	const subQueries = data?.subQueries ?? []
	const finishedCount = subQueries.filter((s) => isQueryFinished(s.state)).length
	const allFinished = data != null && isQueryFinished(data.state) && finishedCount === subQueries.length
	const hasPendingSubQueries = subQueries.length > 0 && finishedCount < subQueries.length
	const rowsSoFar = subQueries.reduce((sum, s) => sum + (s.rowCount ?? 0), 0)
	const rowCount = subQueries.length > 0 ? rowsSoFar : data?.rowCount ?? null

	// Tick every second while queries are still running so that durations count up in real time, rather
	// than freezing at the last server update and making a running query look stuck. Also tick while
	// waiting on a missing query, so the grace period expires without needing another poll.
	const now = useNow(notFound || (data != null && !allFinished))
	const missingTooLong = notFound && notFoundSince != null && now - notFoundSince > NOT_FOUND_GRACE_SECONDS * 1000

	// Calculate wall-clock duration
	const allRecords = data ? [data, ...subQueries] : []
	const startTimes = allRecords.map(r => r.firstUpdateTime).filter((t): t is number => t != null)
	const endTimes = allRecords.map(r => r.lastUpdateTime).filter((t): t is number => t != null)
	const earliestStartTime = startTimes.length > 0 ? Math.min(...startTimes) : null
	const latestEndTime = allFinished
		? endTimes.length > 0 ? Math.max(...endTimes) : null
		: now
	const parentDurationMs = durationBetween(earliestStartTime, latestEndTime)

	useEffect(() => {
		setRefreshInterval(allFinished ? IDLE_REFRESH_SECONDS : ACTIVE_REFRESH_SECONDS)
	}, [allFinished])

	// Non-finished sub-queries first, then most recently updated first.
	const sortedSubQueries = [...subQueries].sort((a, b) => {
		const aFinished = isQueryFinished(a.state)
		const bFinished = isQueryFinished(b.state)
		if (aFinished !== bFinished) return aFinished ? 1 : -1
		return (b.lastUpdateTime ?? 0) - (a.lastUpdateTime ?? 0)
	})

	// Count sub-queries by state so the progress bar can stack a coloured segment per state.
	const stateCounts = subQueries.reduce<Record<string, number>>((counts, sub) => {
		const state = parseQueryState(sub.state)
		counts[state] = (counts[state] ?? 0) + 1
		return counts
	}, {})
	const progressSegments = QUERY_STATES.map((state) => ({
		state,
		count: stateCounts[state] ?? 0,
	})).filter((segment) => segment.count > 0)

	const filterStates = QUERY_STATES.filter(state => (stateCounts[state] ?? 0) > 0 || state === stateFilter)
	const visibleSubQueries = stateFilter
		? sortedSubQueries.filter(q => parseQueryState(q.state) === stateFilter)
		: sortedSubQueries

	return (
		<>
			<Title>{`Query ${queryId}`}</Title>
			<div className="page">
				<div className="query-detail-header">
					<h2>Query</h2>
					<ReloadButton onReload={reload} loading={loading} nextReloadAt={nextReloadAt} />
				</div>

				{notFound ? (
						missingTooLong ? (
							<p className="query-detail-error">
								This query still isn't in the tracker after {NOT_FOUND_GRACE_SECONDS} seconds, so it probably
								doesn't exist. Check the query ID is correct — queries are also removed once they pass the
								tracker's TTL.
							</p>
						) : (
							<p className="query-detail-not-found">
								This query wasn't found in the tracker. If you've just submitted it, it may take a moment to
								be picked up — this page will keep checking and show it shortly.
							</p>
						)
					) : (
						error && <p className="error">Failed to load query: {error}</p>
					)}

				{loading && !data && !error && <p className="query-detail-loading">Loading…</p>}

				{data && (
					<>
						<TrackedQuerySummary
							query={{ ...data, firstUpdateTime: earliestStartTime, rowCount }}
							durationMs={parentDurationMs}
							pending={hasPendingSubQueries}
						/>

						{data.errorMessage && (
							<div className="query-detail-error" role="alert">
								<span className="query-detail-error-title">Error</span>
								<p>{data.errorMessage}</p>
							</div>
						)}

						{subQueries.length > 0 && (
							<div className="query-subqueries">
								<div className="query-subqueries-head">
									<h3 className="query-detail-section-title">Sub-queries ({subQueries.length})</h3>
								</div>

								<div className="query-progress">
									<div
										className="query-progress-bar"
										role="img"
										aria-label={`${finishedCount} of ${subQueries.length} sub-queries finished`}
									>
										{progressSegments.map((segment) => (
											<div
												key={segment.state}
												className={`query-progress-fill ${queryStateModifierClass(segment.state)}`}
												style={{ width: `${(segment.count / subQueries.length) * 100}%` }}
												title={`${segment.count} ${queryStatusLabel(segment.state)}`}
											/>
										))}
									</div>
									<span className="query-progress-label">
										{finishedCount} of {subQueries.length} finished
									</span>
								</div>

								<div className="query-subquery-filters" role="group" aria-label="Filter sub-queries by state">
									<button
										type="button"
										className={stateFilter === null ? 'query-subquery-filter active' : 'query-subquery-filter'}
										aria-pressed={stateFilter === null}
										onClick={() => setStateFilter(null)}
									>
										All <span className="query-subquery-filter-count">{subQueries.length}</span>
									</button>
									{filterStates.map((state) => (
										<button
											key={state}
											type="button"
											className={
												stateFilter === state
													? `query-subquery-filter ${queryStateModifierClass(state)} active`
													: `query-subquery-filter ${queryStateModifierClass(state)}`
											}
											aria-pressed={stateFilter === state}
											onClick={() => setStateFilter(state)}
										>
											{queryStatusLabel(state)}{' '}
											<span className="query-subquery-filter-count">{stateCounts[state] ?? 0}</span>
										</button>
									))}
								</div>

								{stateFilter && visibleSubQueries.length === 0 ? (
									<p className="query-subquery-none">
										No sub-queries are {queryStatusLabel(stateFilter).toLowerCase()}.
									</p>
								) : (
									<table className="query-subquery-table">
										<thead>
											<tr>
												<th>Last updated</th>
												<th>Sub-query ID</th>
												<th>State</th>
												<th>Duration</th>
												<th>Rows</th>
												<th>Results</th>
											</tr>
										</thead>
										<tbody>
											{visibleSubQueries.map((sub) => (
												<tr key={sub.subQueryId}>
													<td>{formatEpochMillis(sub.lastUpdateTime)}</td>
													<td className="query-subquery-id"><code>{sub.subQueryId}</code></td>
													<td>
														<span className={queryStateBadgeClasses(sub.state)}>{queryStatusLabel(sub.state)}</span>
													</td>
													<td className="query-subquery-num">
														{formatDurationMillisSpan(
															durationBetween(
																sub.firstUpdateTime,
																isQueryFinished(sub.state) ? sub.lastUpdateTime : now,
															),
														)}
													</td>
													<td className="query-subquery-num" title={formatNum(sub.rowCount)}>
														{formatRowCount(sub.rowCount)}
													</td>
													<td>
														<QueryDestination
															locations={sub.resultsLocations}
															queryId={queryId}
															subQueryId={sub.subQueryId}
														/>
													</td>
												</tr>
											))}
										</tbody>
									</table>
								)}
							</div>
						)}
					</>
				)}
			</div>
		</>
	)
}
