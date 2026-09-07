import type { ReactNode } from 'react'
import { Link } from 'react-router-dom'
import Spinner from './Spinner'
import { queryStateBadgeClasses, queryStatusLabel } from '../lib/queryStatus'
import { durationBetween, formatDurationMillisSpan, formatEpochMillis } from '../lib/time'
import { formatNum, formatRowCount } from '../lib/queryResults'
import './TrackedQuerySummary.css'

export interface TrackedQueryFacts {
	queryId: string
	subQueryId?: string
	tableId: string | null
	tableName: string | null
	state: string
	firstUpdateTime: number | null
	lastUpdateTime: number | null
	expiryDate: number | null
	rowCount: number | null
}

interface Props {
	query: TrackedQueryFacts
	durationMs?: number | null
	pending?: boolean
}

export default function TrackedQuerySummary({ query, durationMs, pending = false }: Props) {
	const duration = durationMs === undefined ? durationBetween(query.firstUpdateTime, query.lastUpdateTime) : durationMs
	return (
		<dl className="tracked-query-summary">
			{query.subQueryId && (
				<Field label="Sub-query ID">
					<code>{query.subQueryId}</code>
				</Field>
			)}
			<Field label="Query ID">
				{query.subQueryId ? (
					<Link className="tracked-query-summary-link" to={`/queries/${encodeURIComponent(query.queryId)}`}>
						<code>{query.queryId}</code>
					</Link>
				) : (
					<code>{query.queryId}</code>
				)}
			</Field>
			<Field label="Table">{query.tableName ?? query.tableId ?? 'Unknown'}</Field>
			<Field label="State">
				<span className={queryStateBadgeClasses(query.state)}>{queryStatusLabel(query.state)}</span>
			</Field>
			<Field label="Started">{formatEpochMillis(query.firstUpdateTime)}</Field>
			<Field label="Last updated">{formatEpochMillis(query.lastUpdateTime)}</Field>
			<Field label="Duration" pending={pending}>
				{formatDurationMillisSpan(duration)}
			</Field>
			<Field label="Rows" pending={pending} title={formatNum(query.rowCount)}>
				{formatRowCount(query.rowCount)}
			</Field>
			{query.expiryDate && <Field label="Expires">{formatEpochMillis(query.expiryDate)}</Field>}
		</dl>
	)
}

interface FieldProps {
	label: string
	title?: string
	pending?: boolean
	children: ReactNode
}

function Field({ label, title, pending = false, children }: FieldProps) {
	return (
		<div>
			<dt>{label}</dt>
			<dd className={pending ? 'tracked-query-summary-value' : undefined} title={title}>
				{pending && <Spinner label="Pending" />}
				{children}
			</dd>
		</div>
	)
}
