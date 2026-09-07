import type { ReactNode } from 'react'
import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import Title from '../components/Title'
import ReloadButton from '../components/ReloadButton'
import QueryResultsPanel from '../components/QueryResults'
import TrackedQuerySummary from '../components/TrackedQuerySummary'
import { useApi } from '../hooks/useApi'
import { useSelectedTable } from '../hooks/useSelectedTable'
import { isQueryFinished } from '../lib/queryStatus'
import { destinationTypeLabel } from '../lib/queryDestinations'
import { type SubQueryDetailData } from '../lib/queryResults'
import { ACTIVE_REFRESH_SECONDS, IDLE_REFRESH_SECONDS } from './QueryDetail'
import './SubQueryDetail.css'

export default function SubQueryDetail() {
	const { queryId, subQueryId } = useParams<{ queryId: string; subQueryId: string }>()
	const { features, loading } = useSelectedTable()

	if (!queryId || !subQueryId) return <></>

	if (loading && !features) {
		return (
			<Frame>
				<p className="subquery-detail-loading">Loading…</p>
			</Frame>
		)
	}

	if (!features?.QueryStack) {
		return (
			<Frame>
				<div className="queries-unavailable">
					<p>The query stack isn't enabled for this instance.</p>
				</div>
			</Frame>
		)
	}

	return <SubQueryDetailContent queryId={queryId} subQueryId={subQueryId} />
}

function Frame({ children }: { children: ReactNode }) {
	return (
		<>
			<Title>Sub-query</Title>
			<div className="page">
				<h2>Sub-query</h2>
				{children}
			</div>
		</>
	)
}

function SubQueryDetailContent({ queryId, subQueryId }: { queryId: string; subQueryId: string }) {
	const [refreshInterval, setRefreshInterval] = useState(ACTIVE_REFRESH_SECONDS)
	const { data, loading, error, errorStatus, reload, nextReloadAt } = useApi<SubQueryDetailData>(
		`/query/${encodeURIComponent(queryId)}/${encodeURIComponent(subQueryId)}`,
		refreshInterval,
	)

	const finished = data != null && isQueryFinished(data.state)
	useEffect(() => {
		setRefreshInterval(finished ? IDLE_REFRESH_SECONDS : ACTIVE_REFRESH_SECONDS)
	}, [finished])

	const s3Locations = data?.resultsLocations?.filter((l) => l.type === 's3') ?? []
	const otherLocations = data?.resultsLocations?.filter((l) => l.type !== 's3') ?? []

	return (
		<>
			<Title>{`Sub-query ${subQueryId}`}</Title>
			<div className="page">
				<div className="subquery-detail-header">
					<h2>Sub-query</h2>
					<ReloadButton onReload={reload} loading={loading} nextReloadAt={nextReloadAt} />
				</div>

				{errorStatus === 404 ? (
					<p className="subquery-detail-not-found">
						This sub-query wasn't found on query <code>{queryId}</code>. Sub-queries are removed once they
						pass the query tracker's TTL.
					</p>
				) : (
					error && <p className="error">Failed to load sub-query: {error}</p>
				)}

				{loading && !data && !error && <p className="subquery-detail-loading">Loading…</p>}

				{data && errorStatus !== 404 && (
					<>
						<TrackedQuerySummary query={data} />

						{data.errorMessage && (
							<div className="subquery-detail-error" role="alert">
								<span className="subquery-detail-error-title">Error</span>
								<p>{data.errorMessage}</p>
							</div>
						)}

						{data.resultsLocations.length === 0 ? (
							<p className="subquery-detail-no-results">
								{finished
									? 'No results destinations were recorded for this sub-query.'
									: 'Results will be available once this sub-query has finished.'}
							</p>
						) : (
							<>
								<h3 className="subquery-detail-results-title">
									Results {s3Locations.length > 1 && `(${s3Locations.length} files)`}
								</h3>

								{s3Locations.map(loc => (
									<QueryResultsPanel
										key={loc.location}
										queryId={queryId}
										subQueryId={subQueryId}
										location={loc.location}
										autoLoad={s3Locations.length === 1}
										maxRows={data.maxResultRows}
									/>
								))}

								{otherLocations.length > 0 && (
									<div className="subquery-detail-other-destinations">
										<p>
											{s3Locations.length > 0
												? 'Results were also sent to destinations that can\'t be displayed here:'
												: 'Results were sent to destinations that can\'t be displayed here — they are consumed as they\'re read:'}
										</p>
										<ul>
											{otherLocations.map((loc, i) => (
												<li key={`${loc.type}-${i}`}>
													<span className="subquery-detail-destination-type">
														{destinationTypeLabel(loc.type)}
													</span>
													<code>{loc.location}</code>
												</li>
											))}
										</ul>
									</div>
								)}
							</>
						)}
					</>
				)}
			</div>
		</>
	)
}
