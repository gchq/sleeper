import type { ReactNode } from 'react'
import { useCallback, useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import Title from '../components/Title'
import IngestFileWizard from '../components/IngestFileWizard'
import ReloadButton from '../components/ReloadButton'
import IngestBatcherArchitecture from '../components/architectures/IngestBatcherArchitecture'
import { useApi } from '../hooks/useApi'
import { useSelectedTable } from '../hooks/useSelectedTable'
import { formatBytes } from '../lib/dataMetrics'
import { formatDurationSeconds, formatTimestamp } from '../lib/time'
import { parseLimit } from '../lib/pagination'
import './IngestBatcher.css'
import { TableStatus } from '../contexts/InstanceContext'

type Mode = 'pending' | 'all'

const DEFAULT_LIMIT = 100
const PATH_DEBOUNCE_MS = 400

interface BatcherFile {
	file: string
	fileSizeBytes: number
	tableId: string
	tableName: string | null
	receivedTime: string
	jobId: string | null
}

interface BatcherFilesResponse {
	files: BatcherFile[]
	limit: number
	hasMore: boolean
}

interface BatchConfig {
	tableId: string | null
	minJobSize: string
	maxJobSize: string
	minJobFiles: string
	maxJobFiles: string
	maxFileAgeSeconds: string
	ingestQueue: string
	jobCreationPeriodMinutes: string
	defaultsOverridden: boolean
}

function ingestMethodLabel(queue: string): string {
	switch (queue.toLowerCase()) {
		case 'standard_ingest':
			return 'standard ingest'
		case 'bulk_import_emr':
			return 'bulk import (EMR)'
		case 'bulk_import_persistent_emr':
			return 'bulk import (persistent EMR)'
		case 'bulk_import_eks':
			return 'bulk import (EKS)'
		case 'bulk_import_emr_serverless':
			return 'bulk import (EMR Serverless)'
		default:
			return queue
	}
}

export default function IngestBatcher() {
	const { table, features, loading } = useSelectedTable()

	if (loading && !features) {
		return (
			<Frame>
				<p className="batcher-loading">Loading…</p>
			</Frame>
		)
	}

	if (!features?.IngestBatcherStack) {
		return (
			<Frame>
				<div className="batcher-unavailable">
					<p>The Ingest Batcher component isn't available for this instance.</p>
					<p>
						To enable it, add <code>IngestBatcherStack</code> to the{' '}
						<code>sleeper.optional.stacks</code> instance property, then redeploy the instance.
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

	return <IngestBatcherContent table={table} />
}

function Frame({ children }: { children: ReactNode }) {
	return (
		<>
			<Title>Ingest Batcher</Title>
			<div className="page">
				<h2>Ingest Batcher</h2>
				{children}
			</div>
		</>
	)
}

function IngestBatcherContent({ table }: { table?: TableStatus }) {
	const [searchParams, setSearchParams] = useSearchParams()
	const mode: Mode = searchParams.get('mode') === 'all' ? 'all' : 'pending'
	const pathFilter = searchParams.get('path') ?? ''
	const limit = parseLimit(Number(searchParams.get('limit') ?? DEFAULT_LIMIT), DEFAULT_LIMIT)

	const [pathInput, setPathInput] = useState(pathFilter)
	const [ingestOpen, setIngestOpen] = useState(false)

	const query = new URLSearchParams({ mode, limit: String(limit) })
	if (pathFilter) query.set('path', pathFilter)
	if (table?.tableUniqueId) query.set('tableId', table.tableUniqueId)

	const { data, loading, error, reload, nextReloadAt } = useApi<BatcherFilesResponse>('/ingest-batcher/files?' + query)
	const files = data?.files ?? []
	const hasMore = data?.hasMore ?? false
	// The API caps the page size. When it hands back a smaller limit than we asked for, we've hit
	// that cap and asking for more would return the same page again.
	const atMaxLimit = data != null && data.limit < limit

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

	// Debounce the path filter into the URL, resetting the limit when it changes
	useEffect(() => {
		if (pathInput === pathFilter) return
		const id = setTimeout(() => {
			setSearchParams(
				(prev) => {
					const next = new URLSearchParams(prev)
					if (pathInput) next.set('path', pathInput)
					else next.delete('path')
					next.delete('limit')
					return next
				},
				{ replace: true },
			)
		}, PATH_DEBOUNCE_MS)
		return () => clearTimeout(id)
	}, [pathInput, pathFilter, setSearchParams])

	function changeMode(next: Mode) {
		if (next === mode) return
		setSearchParams((prev) => {
			const params = new URLSearchParams(prev)
			params.set('mode', next)
			params.delete('limit')
			return params
		})
	}

	const heading = table?.tableName ? `Ingest Batcher: ${table.tableName}` : 'Ingest Batcher'

	return (
		<>
			<Title>{heading}</Title>
			<div className="page">
				<h2>{heading}</h2>

				<IngestBatcherArchitecture />

				<div className="batcher-controls">
					<div className="batcher-mode" role="group" aria-label="File view">
						<button
							type="button"
							className={mode === 'pending' ? 'batcher-mode-btn active' : 'batcher-mode-btn'}
							onClick={() => changeMode('pending')}
						>
							Pending (oldest first)
						</button>
						<button
							type="button"
							className={mode === 'all' ? 'batcher-mode-btn active' : 'batcher-mode-btn'}
							onClick={() => changeMode('all')}
						>
							All files (newest first)
						</button>
					</div>
					<input
						type="search"
						className="batcher-path-filter"
						placeholder="Filter by file path…"
						value={pathInput}
						onChange={(e) => setPathInput(e.target.value)}
						aria-label="Filter by file path"
					/>
					<ReloadButton onReload={reload} loading={loading} nextReloadAt={nextReloadAt} />
					<button type="button" className="btn btn-primary batcher-ingest-file" onClick={() => setIngestOpen(true)}>
						Ingest File
					</button>
				</div>

				{ingestOpen && (
					<IngestFileWizard
						onClose={() => setIngestOpen(false)}
						onSubmitted={reload}
						defaultMethod="ingest_batcher"
						presetTableId={table?.tableUniqueId}
					/>
				)}

				{mode === 'pending' && <BatchInfoBanner table={table} />}

				{error && <p className="error">Failed to load ingest batcher files: {error}</p>}

				<table className="batcher-table">
					<thead>
						<tr>
							<th>Received</th>
							<th>File</th>
							{!table && <th>Table</th>}
							<th>Size</th>
							<th>Status</th>
						</tr>
					</thead>
					<tbody>
						{files.map((f) => (
							<tr key={f.tableId + '|' + f.file}>
								<td>{formatTimestamp(f.receivedTime)}</td>
								<td className="batcher-file"><code>{f.file}</code></td>
								{!table && <td>{f.tableName ?? f.tableId}</td>}
								<td className="batcher-size">{formatBytes(f.fileSizeBytes)}</td>
								<td>
									{f.jobId ? (
										<span className="batcher-status assigned" title={`Job ${f.jobId}`}>
											Assigned
										</span>
									) : (
										<span className="batcher-status pending">Pending</span>
									)}
								</td>
							</tr>
						))}
					</tbody>
				</table>

				{!loading && files.length === 0 && !error && <p className="batcher-empty">No files found.</p>}
				{loading && <p className="batcher-loading">Loading…</p>}

				{hasMore && !loading && (
					<div className="batcher-load-more">
						{atMaxLimit ? (
							<p className="batcher-limit-reached">
								Showing the first {files.length.toLocaleString()} files, the most this page will load. Narrow the
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

function BatchInfoBanner({ table }: { table?: TableStatus }) {
	const query = table ? '?tableId=' + encodeURIComponent(table.tableUniqueId) : ''
	const { data: config } = useApi<BatchConfig>('/ingest-batcher/config' + query)

	if (!config) return null

	const method = ingestMethodLabel(config.ingestQueue)

	const propertiesLink = table
		? `/tables/${encodeURIComponent(table.tableUniqueId)}/properties?filter=ingest.batcher&editable`
		: '/instance/properties?filter=ingest.batcher&editable'

	return (
		<div className="batcher-info">
			<div className="batcher-info-header">
				<span className="batcher-info-title">Ingest batching configuration</span>
				<Link className="btn batcher-change" to={propertiesLink}>
					Change
				</Link>
			</div>

			{!table ? (
				<p>
					Default configuration (can be overridden per table):
				</p>
			) : config.defaultsOverridden ? (
				<p>
					<strong>{table?.tableName}</strong> uses table specific config:
				</p>
			) : (
				<p>
					<strong>{table?.tableName}</strong> uses the instance default config:
				</p>
			)}

			<p>
				Every <strong>{config.jobCreationPeriodMinutes}</strong> minute{config.jobCreationPeriodMinutes === '1' ? '' : 's'}
				,{' '}any pending files are batched into ingest jobs when the total size of waiting files reaches{' '}
				<strong>{config.minJobSize}</strong> <em>and</em> at least <strong>{config.minJobFiles}</strong> file
				{config.minJobFiles === '1' ? ' is' : 's are'} waiting, or as soon as any file has been waiting longer than{' '}
				<strong>{formatDurationSeconds(config.maxFileAgeSeconds)}</strong>. A single job holds at most{' '}
				<strong>{config.maxJobSize}</strong> of data and <strong>{config.maxJobFiles}</strong> files.
				Jobs are sent for ingest using <strong>{method}</strong>.
			</p>
		</div>
	)
}
