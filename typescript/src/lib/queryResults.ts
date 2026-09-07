import { formatCount, formatCountCompact } from './dataMetrics'

export interface ResultsLocation {
	type: string
	location: string
}

export interface SubQueryView {
	subQueryId: string
	state: string
	firstUpdateTime: number | null
	lastUpdateTime: number | null
	expiryDate: number | null
	rowCount: number | null
	errorMessage: string | null
	resultsLocations: ResultsLocation[]
}

export interface QueryDetailData {
	queryId: string
	tableId: string | null
	tableName: string | null
	state: string
	firstUpdateTime: number | null
	lastUpdateTime: number | null
	expiryDate: number | null
	rowCount: number | null
	errorMessage: string | null
	subQueries: SubQueryView[]
}

export interface SubQueryDetailData extends SubQueryView {
	queryId: string
	tableId: string | null
	tableName: string | null
	maxResultRows: number
}

export interface QueryResultsData {
	columns: string[]
	rows: Record<string, unknown>[]
	truncated: boolean
}

export interface ResultsViewOption {
	label: string
	toText?: (data: QueryResultsData) => string
}

export const RESULTS_VIEWS = {
	table: { label: 'Table' },
	csv: { label: 'CSV', toText: data => toCsv(data.columns, data.rows) },
	json: { label: 'JSON', toText: data => JSON.stringify(data.rows, null, 2) },
	jsonl: { label: 'JSONL', toText: data => data.rows.map(row => JSON.stringify(row)).join('\n') },
} satisfies Record<string, ResultsViewOption>

export type ResultsView = keyof typeof RESULTS_VIEWS

export const RESULT_LIMITS = [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]

export function formatNum(value: number | null): string {
	return value == null ? '—' : value.toLocaleString()
}

const DEFAULT_COMPACT_ROW_COUNT_FROM = 10_000_000

export function formatRowCount(value: number | null, compactFrom = DEFAULT_COMPACT_ROW_COUNT_FROM): string {
	return value != null && Math.abs(value) >= compactFrom ? formatCountCompact(value) : formatCount(value)
}

export function formatCell(value: unknown): string {
	if (value == null) return '—'
	if (typeof value === 'object') return JSON.stringify(value)
	return String(value)
}

export function formatCsvCell(value: unknown): string {
	if (value == null) return ''
	const text = typeof value === 'object' ? JSON.stringify(value) : String(value)
	// RFC 4180: quote fields containing a delimiter, quote or newline, and double any inner quotes
	return /[",\r\n]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text
}

export function toCsv(columns: string[], rows: Record<string, unknown>[]): string {
	const header = columns.map(formatCsvCell).join(',')
	const body = rows.map((row) => columns.map((col) => formatCsvCell(row[col])).join(','))
	return [header, ...body].join('\n')
}
