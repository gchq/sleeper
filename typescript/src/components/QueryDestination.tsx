import { Link } from 'react-router-dom'
import { destinationsFor } from '../lib/queryDestinations'
import type { ResultsLocation } from '../lib/queryResults'
import './QueryDestination.css'

const ICON_SIZE = 18

export default function QueryDestination({
	queryId,
	subQueryId,
	locations,
}: {
	queryId: string
	subQueryId: string
	locations: ResultsLocation[]
}) {
	const destinations = destinationsFor(locations)
	if (destinations.length === 0) return <>—</>

	const href = `/queries/${encodeURIComponent(queryId)}/${encodeURIComponent(subQueryId)}`

	return (
		<span className="query-destination-icons">
			{destinations.map(({ kind, label, icon: Icon }) => (
				<Link
					key={kind}
					className="query-destination-icon"
					to={href}
					title={`${label} — view sub-query results`}
					aria-label={`${label} — view sub-query results`}
				>
					<Icon width={ICON_SIZE} height={ICON_SIZE} />
				</Link>
			))}
		</span>
	)
}
