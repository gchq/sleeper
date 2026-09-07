import { AmazonDynamoDb, AwsLambda, AmazonSimpleQueueService, AmazonSimpleStorageService } from '@aws-icons/react/architecture-service'
import Architecture, { type Edge, type NodeConfig } from './Architecture'

type ResourceKey =
	| 'queryQueue'
	| 'queryDLQ'
	| 'queryFunction'
	| 'subQueryQueue'
	| 'failureQueue'
	| 'failureFunction'
	| 'subQueryDLQ'
	| 'subQueryFunction'
	| 'resultsBucket'
	| 'trackerTable'

const NODES = {
	queryQueue: { shortTitle: 'Queries', longTitle: 'Query Queue', col: 1, row: 2, icon: AmazonSimpleQueueService },
	queryDLQ: { shortTitle: 'Query DLQ', longTitle: 'Query Dead Letter Queue', col: 1, row: 3, icon: AmazonSimpleQueueService },
	queryFunction: { shortTitle: 'Query Planner', longTitle: 'Query Lambda Function', col: 2, row: 2, icon: AwsLambda },
	subQueryQueue: { shortTitle: 'SubQueries', longTitle: 'SubQuery Queue', col: 3, row: 2, icon: AmazonSimpleQueueService },
	failureQueue: { shortTitle: 'SubQuery Failures', longTitle: 'SubQuery Failure Queue', col: 3, row: 3, icon: AmazonSimpleQueueService },
	failureFunction: { shortTitle: 'Failure Processor', longTitle: 'Failure Processing Lambda Function', col: 4, row: 3, icon: AwsLambda },
	subQueryDLQ: { shortTitle: 'SubQuery DLQ', longTitle: 'SubQuery Dead Letter Queue', col: 5, row: 3, icon: AmazonSimpleQueueService },
	subQueryFunction: { shortTitle: 'Query Executor', longTitle: 'SubQuery Lambda Function', col: 4, row: 2, icon: AwsLambda },
	resultsBucket: { shortTitle: 'Results', longTitle: 'Query Results Bucket', col: 5, row: 2, icon: AmazonSimpleStorageService },
	trackerTable: { shortTitle: 'Tracking', longTitle: 'Query Tracker Table', col: 3, row: 1, icon: AmazonDynamoDb },
} satisfies Record<ResourceKey, NodeConfig>

const EDGES: Edge<ResourceKey>[] = [
	{ from: 'queryQueue', to: 'queryFunction' },
	{ from: 'queryQueue', to: 'queryDLQ', errorPath: true },
	{ from: 'queryFunction', to: 'trackerTable' },
	{ from: 'queryFunction', to: 'subQueryQueue' },
	{ from: 'subQueryQueue', to: 'subQueryFunction' },
	{ from: 'subQueryQueue', to: 'failureQueue', errorPath: true },
	{ from: 'failureQueue', to: 'failureFunction', errorPath: true },
	{ from: 'failureFunction', to: 'subQueryDLQ', errorPath: true },
	{ from: 'subQueryFunction', to: 'trackerTable' },
	{ from: 'subQueryFunction', to: 'resultsBucket' },
]

export default function QueryArchitecture() {
	return <Architecture url="/query/resources" nodes={NODES} edges={EDGES} initiallyCollapsed={true} />
}
