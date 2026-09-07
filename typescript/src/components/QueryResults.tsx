import { useCallback, useEffect, useRef, useState } from 'react'
import { apiFetch } from '../lib/api'
import { useInstance } from '../contexts/InstanceContext'
import { useCopyToClipboard } from '../hooks/useCopyToClipboard'
import { s3ObjectConsoleUrl } from '../lib/aws'
import {
	formatCell,
	RESULT_LIMITS,
	RESULTS_VIEWS,
	type QueryResultsData,
	type ResultsView,
	type ResultsViewOption,
} from '../lib/queryResults'
import './QueryResults.css'

export default function QueryResults({
	queryId,
	subQueryId,
	location,
	autoLoad,
	maxRows,
}: {
	queryId: string
	subQueryId: string
	location: string
	autoLoad: boolean
	maxRows: number
}) {
	const { region } = useInstance()
	const [limit, setLimit] = useState(RESULT_LIMITS[0])
	const [view, setView] = useState<ResultsView>('table')
	const { copiedKey, copy } = useCopyToClipboard()
	
	const [data, setData] = useState<QueryResultsData | null>(null)
	const [loading, setLoading] = useState(false)
	const [error, setError] = useState<string | null>(null)
	const [loaded, setLoaded] = useState(false)
	
	const consoleUrl = region ? s3ObjectConsoleUrl(location, region) : null
	const limits = RESULT_LIMITS.filter((rows) => rows <= maxRows)
	
	const viewOption: ResultsViewOption = RESULTS_VIEWS[view]
	const resultsAsText = data && viewOption.toText ? viewOption.toText(data) : ''

	const limitRef = useRef(limit)
	useEffect(() => {
		limitRef.current = limit
	}, [limit])

	const loadResults = useCallback(async (nextLimit = limitRef.current) => {
		setLoading(true)
		setError(null)
		try {
			const params = new URLSearchParams({ limit: String(nextLimit), location })
			const resp = await apiFetch(
				`/query/${encodeURIComponent(queryId)}/${encodeURIComponent(subQueryId)}/results?${params.toString()}`,
			)
			if (!resp.ok) {
				setError(`HTTP ${resp.status}`)
				return
			}
			setData(await resp.json())
			setLoaded(true)
		} catch (err) {
			setError((err as Error).message)
		} finally {
			setLoading(false)
		}
	}, [queryId, subQueryId, location])

	useEffect(() => {
		if (autoLoad) loadResults()
	}, [autoLoad, loadResults])

	function changeLimit(next: number) {
		setLimit(next)
		if (loaded) loadResults(next)
	}

	return (
		<div className="query-results">
			<div className="query-results-head">
				<h3 className="query-results-title">
					<code className="query-results-path">{location}</code>
				</h3>
				<div className="query-results-controls">
					<label className="query-results-limit">
						<span>Rows</span>
						<select value={limit} onChange={e => changeLimit(Number(e.target.value))}>
							{limits.map(n => (
								<option key={n} value={n}>
									{n}
								</option>
							))}
						</select>
					</label>

					<div className="query-results-view" role="group" aria-label="Results view">
						{(Object.entries(RESULTS_VIEWS) as [ResultsView, ResultsViewOption][]).map(([v, option]) => (
							<button
								key={v}
								type="button"
								className={view === v ? 'query-results-view-btn active' : 'query-results-view-btn'}
								aria-pressed={view === v}
								onClick={() => setView(v)}
							>
								{option.label}
							</button>
						))}
					</div>

					<div className="query-results-actions">
						{consoleUrl && (
							<a className="query-results-s3-link" href={consoleUrl} target="_blank" rel="noreferrer">
								View in S3
							</a>
						)}

						<button type="button" className="btn btn-primary" onClick={() => loadResults()} disabled={loading}>
							{loading ? 'Loading…' : loaded ? 'Reload' : 'View results'}
						</button>
					</div>
				</div>
			</div>

			{error && <p className="error">Failed to load results: {error}</p>}

			{data && (
				<>
					{data.truncated && (
						<p className="query-results-note">Showing the first {data.rows.length.toLocaleString()} rows.</p>
					)}
					{data.rows.length === 0 ? (
						<p className="query-results-empty">This results file is empty.</p>
					) : view === 'table' ? (
						<div className="query-results-table-wrap">
							<table className="query-results-table">
								<thead>
									<tr>
										{data.columns.map((col) => (
											<th key={col}>{col}</th>
										))}
									</tr>
								</thead>
								<tbody>
									{data.rows.map((row, i) => (
										<tr key={i}>
											{data.columns.map((col) => (
												<td key={col}>{formatCell(row[col])}</td>
											))}
										</tr>
									))}
								</tbody>
							</table>
						</div>
					) : (
						<div className="query-results-text">
							<button
								type="button"
								className="query-results-copy"
								onClick={() => copy(resultsAsText, 'results')}
							>
								{copiedKey === 'results' ? 'Copied' : `Copy ${viewOption.label}`}
							</button>
							<pre className="query-results-json">{resultsAsText}</pre>
						</div>
					)}
				</>
			)}
		</div>
	)
}
