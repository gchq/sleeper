
// Order here is the order they will be displayed in the progress bar.
export const QUERY_STATES = ['COMPLETED', 'PARTIALLY_FAILED', 'FAILED', 'IN_PROGRESS', 'QUEUED'] as const

export const QUERY_STATES_IN_LIFECYCLE_ORDER: QueryState[] = [
	'QUEUED',
	'IN_PROGRESS',
	'COMPLETED',
	'FAILED',
	'PARTIALLY_FAILED',
]

export type QueryState = (typeof QUERY_STATES)[number]

export function parseQueryState(state: string): QueryState {
	const upper = state.toUpperCase() as QueryState
	return QUERY_STATES.includes(upper) ? upper : 'QUEUED'
}

export type QueryStateFilter = 'all' | Lowercase<QueryState>

export function queryStateFilterFor(state: QueryState): QueryStateFilter {
	return state.toLowerCase() as Lowercase<QueryState>
}

export function parseQueryStateFilter(value: string | null): QueryStateFilter {
	const state = QUERY_STATES.find((s) => queryStateFilterFor(s) === value)
	return state ? queryStateFilterFor(state) : 'all'
}

export function queryStatusLabel(state: string): string {
	return state
		.toLowerCase()
		.split('_')
		.map(word => word.charAt(0).toUpperCase() + word.slice(1))
		.join(' ')
}

export function isQueryFinished(state: string): boolean {
	switch (parseQueryState(state)) {
		case 'COMPLETED':
		case 'FAILED':
		case 'PARTIALLY_FAILED':
			return true
		default:
			return false
	}
}

export function queryStateModifierClass(state: string): string {
	return parseQueryState(state).toLowerCase().replace(/_/g, '-')
}

export function queryStateBadgeClasses(state: string): string {
	return 'query-status ' + queryStateModifierClass(state)
}
