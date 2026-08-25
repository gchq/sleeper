import type { ReactNode } from 'react'
import { Link, useParams } from 'react-router-dom'
import Title from '../components/Title'
import ReloadButton from '../components/ReloadButton'
import { useApi } from '../hooks/useApi'
import { useSelectedTable } from '../hooks/useSelectedTable'
import { formatDurationSeconds, formatTimestamp } from '../lib/time'
import { statusClass, statusLabel } from '../lib/ingestStatus'
import './IngestJobDetail.css'

interface IngestTaskView {
	taskId: string
	startTime: string | null
	finishTime: string | null
	durationSeconds: number | null
	finished: boolean
	totalRowsRead: number | null
	totalRowsWritten: number | null
	timeSpentOnJobsSeconds: number | null
}

interface IngestJobRunView {
	taskId: string | null
	status: string
	startTime: string | null
	finishTime: string | null
	finished: boolean
	finishedSuccessfully: boolean
	rowsRead: number | null
	rowsWritten: number | null
	durationSeconds: number | null
	failureReasons: string[]
}

interface IngestJobDetail {
	jobId: string
	tableId: string | null
	tableName: string | null
	inputFileCount: number
	expiryDate: string | null
	status: string
	runs: IngestJobRunView[]
	tasks: Record<string, IngestTaskView>
}

function formatNum(value: number | null): string {
	return value == null ? '—' : value.toLocaleString()
}

function formatTime(value: string | null): string {
	return value ? formatTimestamp(value) : '—'
}

function formatDuration(value: number | null): string {
	return value == null ? '—' : formatDurationSeconds(Math.round(value))
}

export default function IngestJobDetail() {
	const { jobId } = useParams<{ jobId: string }>()
	const { table, features, loading } = useSelectedTable()
	if (!jobId) return <></>

	const backLink = table?.tableUniqueId ? `/tables/${encodeURIComponent(table.tableUniqueId)}/ingest-jobs` : '/ingest-jobs'

	if (loading && !features) {
		return (
			<Frame backLink={backLink}>
				<p className="job-detail-loading">Loading…</p>
			</Frame>
		)
	}

	if (!features?.IngestTracking) {
		return (
			<Frame backLink={backLink}>
				<div className="jobs-unavailable">
					<p>Ingest tracking isn't enabled for this instance.</p>
				</div>
			</Frame>
		)
	}

	return <IngestJobDetailContent jobId={jobId} backLink={backLink} />
}

function Frame({ children, backLink }: { children: ReactNode; backLink: string }) {
	return (
		<>
			<Title>Ingest Job</Title>
			<div className="page">
				<Link className="job-detail-back" to={backLink}>
					← Back to Ingest Jobs
				</Link>
				<h2>Ingest Job</h2>
				{children}
			</div>
		</>
	)
}

function IngestJobDetailContent({
	jobId,
	backLink,
}: {
	jobId: string
	backLink: string
}) {
	const { data, loading, error, reload, nextReloadAt } = useApi<IngestJobDetail>(
		'/ingest-tracking/job/' + encodeURIComponent(jobId),
	)

	return (
		<>
			<Title>{`Ingest Job ${jobId}`}</Title>
			<div className="page">
				<Link className="job-detail-back" to={backLink}>
					← Back to Ingest Jobs
				</Link>

				<div className="job-detail-header">
					<h2>Ingest Job</h2>
					<ReloadButton onReload={reload} loading={loading} nextReloadAt={nextReloadAt} />
				</div>

				{error && <p className="error">Failed to load ingest job: {error}</p>}
				{loading && !data && <p className="job-detail-loading">Loading…</p>}

				{data && (
					<>
						<dl className="job-detail-summary">
							<div>
								<dt>Job ID</dt>
								<dd>
									<code>{data.jobId}</code>
								</dd>
							</div>
							<div>
								<dt>Table</dt>
								<dd>{data.tableName ?? data.tableId ?? 'Unknown'}</dd>
							</div>
							<div>
								<dt>Status</dt>
								<dd>
									<span className={statusClass(data.status)}>{statusLabel(data.status)}</span>
								</dd>
							</div>
							<div>
								<dt>Input files</dt>
								<dd>{data.inputFileCount.toLocaleString()}</dd>
							</div>
							<div>
								<dt>Runs</dt>
								<dd>{data.runs.length.toLocaleString()}</dd>
							</div>
							{data.expiryDate && (
								<div>
									<dt>Expires</dt>
									<dd>{formatTime(data.expiryDate)}</dd>
								</div>
							)}
						</dl>

						<h3 className="job-detail-section-title">Runs</h3>
						{data.runs.length === 0 ? (
							<p className="job-detail-empty">This job has not run yet.</p>
						) : (
							<div className="job-detail-runs">
								{data.runs.map((run, i) => (
									<RunCard
										key={i}
										run={run}
										task={run.taskId ? data.tasks[run.taskId] : undefined}
									/>
								))}
							</div>
						)}
					</>
				)}
			</div>
		</>
	)
}

function RunCard({ run, task }: { run: IngestJobRunView; task?: IngestTaskView }) {
	return (
		<div className="job-run-card">
			<div className="job-run-head">
				<span className={statusClass(run.status)}>{statusLabel(run.status)}</span>
				{run.taskId && (
					<span className="job-run-task-id" title="Task ID">
						Task <code>{run.taskId}</code>
					</span>
				)}
			</div>

			<dl className="job-run-grid">
				<div>
					<dt>Started</dt>
					<dd>{formatTime(run.startTime)}</dd>
				</div>
				<div>
					<dt>Finished</dt>
					<dd>{formatTime(run.finishTime)}</dd>
				</div>
				<div>
					<dt>Duration</dt>
					<dd>{formatDuration(run.durationSeconds)}</dd>
				</div>
				<div>
					<dt>Rows read</dt>
					<dd>{formatNum(run.rowsRead)}</dd>
				</div>
				<div>
					<dt>Rows written</dt>
					<dd>{formatNum(run.rowsWritten)}</dd>
				</div>
			</dl>

			{run.failureReasons.length > 0 && (
				<div className="job-run-failures">
					<span className="job-run-failures-title">Failure reasons</span>
					<ul>
						{run.failureReasons.map((reason, i) => (
							<li key={i}>{reason}</li>
						))}
					</ul>
				</div>
			)}

			{task && (
				<div className="job-run-task">
					<span className="job-run-task-title">Task</span>
					<dl className="job-run-grid">
						<div>
							<dt>Task started</dt>
							<dd>{formatTime(task.startTime)}</dd>
						</div>
						<div>
							<dt>Task finished</dt>
							<dd>{formatTime(task.finishTime)}</dd>
						</div>
						<div>
							<dt>Task duration</dt>
							<dd>{formatDuration(task.durationSeconds)}</dd>
						</div>
						<div>
							<dt>Task total rows read</dt>
							<dd>{formatNum(task.totalRowsRead)}</dd>
						</div>
						<div>
							<dt>Task total rows written</dt>
							<dd>{formatNum(task.totalRowsWritten)}</dd>
						</div>
					</dl>
				</div>
			)}
		</div>
	)
}
