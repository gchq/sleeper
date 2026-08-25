import { AmazonDynamoDb } from '@aws-icons/react/architecture-service'
import Architecture, { type Edge, type NodeConfig } from './Architecture'

type ResourceKey =
	| 'jobLookupTable'
	| 'jobUpdatesTable'
	| 'taskUpdatesTable'

const NODES = {
	jobLookupTable: { shortTitle: 'Job Lookup', longTitle: 'Job Lookup Table', col: 1, row: 1, icon: AmazonDynamoDb },
	jobUpdatesTable: { shortTitle: 'Job Updates', longTitle: 'Job Updates Table', col: 2, row: 1, icon: AmazonDynamoDb },
	taskUpdatesTable: { shortTitle: 'Task Updates', longTitle: 'Task Updates Table', col: 3, row: 1, icon: AmazonDynamoDb },
} satisfies Record<ResourceKey, NodeConfig>

const EDGES: Edge<ResourceKey>[] = []

export default function IngestTrackingArchitecture() {
	return <Architecture url="/ingest-tracking/resources" nodes={NODES} edges={EDGES} initiallyCollapsed={true} />
}
