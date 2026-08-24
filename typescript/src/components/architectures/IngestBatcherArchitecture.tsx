import {
	AmazonSimpleQueueService,
	AwsLambda,
	AmazonDynamoDb,
	AmazonEventBridge,
} from '@aws-icons/react/architecture-service'
import Architecture, { type Edge, type NodeConfig } from './Architecture'

type ResourceKey =
	| 'submitQueue'
	| 'submitDLQ'
	| 'submitterLambda'
	| 'trackingTable'
	| 'creationScheduler'
	| 'jobCreatorLambda'
	| 'ingestTarget'

const NODES = {
	submitQueue: { shortTitle: 'Submit Queue', longTitle: 'Submit Queue', col: 1, row: 1, icon: AmazonSimpleQueueService },
	submitDLQ: { shortTitle: 'Submit DLQ', longTitle: 'Submit Dead Letter Queue', col: 1, row: 2, icon: AmazonSimpleQueueService },
	submitterLambda: { shortTitle: 'Submitter', longTitle: 'Submitter Lambda', col: 2, row: 1, icon: AwsLambda },
	trackingTable: { shortTitle: 'Tracking', longTitle: 'Tracking Table', col: 3, row: 1, icon: AmazonDynamoDb },
	creationScheduler: { shortTitle: 'Scheduler', longTitle: 'Creation Scheduler', col: 2, row: 2, icon: AmazonEventBridge },
	jobCreatorLambda: { shortTitle: 'Job Creator', longTitle: 'Job Creator Lambda', col: 3, row: 2, icon: AwsLambda },
	ingestTarget: { shortTitle: 'Ingest', longTitle: 'Ingest', col: 4, row: 2, icon: AmazonSimpleQueueService },
} satisfies Record<ResourceKey, NodeConfig>

const EDGES: Edge<ResourceKey>[] = [
	{ from: 'submitQueue', to: 'submitterLambda' },
	{ from: 'submitQueue', to: 'submitDLQ', errorPath: true },
	{ from: 'submitterLambda', to: 'trackingTable' },
	{ from: 'creationScheduler', to: 'jobCreatorLambda' },
	{ from: 'trackingTable', to: 'jobCreatorLambda' },
	{ from: 'jobCreatorLambda', to: 'ingestTarget' },
]

export default function IngestBatcherArchitecture() {
	return <Architecture url="/ingest-batcher/resources" nodes={NODES} edges={EDGES} />
}
