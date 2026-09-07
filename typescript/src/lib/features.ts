import type { ComponentType } from 'react'
import {
	AmazonApiGateway,
	AmazonAthena,
	AmazonCloudWatch,
	AmazonDynamoDb,
	AmazonElasticContainerService,
	AmazonElasticKubernetesService,
	AmazonEmr,
	AmazonEventBridge,
	AmazonSimpleQueueService,
	AmazonSimpleStorageService,
	AwsAppStudio,
	AwsFargate,
	AwsLambda,
} from '@aws-icons/react/architecture-service'
import GarbageCollectionIcon from '../components/icons/GarbageCollectionIcon'
import PartitionSplittingIcon from '../components/icons/PartitionSplittingIcon'
import type { IconProps } from '../components/icons/icon'

export const OPTIONAL_STACKS = [
	'IngestStack',
	'IngestBatcherStack',
	'EmrServerlessBulkImportStack',
	'EmrBulkImportStack',
	'PersistentEmrBulkImportStack',
	'EksBulkImportStack',
	'EmrStudioStack',
	'BulkExportStack',
	'QueryStack',
	'WebSocketQueryStack',
	'AthenaStack',
	'KeepLambdaWarmStack',
	'CompactionStack',
	'GarbageCollectorStack',
	'PartitionSplittingStack',
	'RestApiStack',
	'DashboardStack',
	'TableMetricsStack',
] as const

export type OptionalStackName = (typeof OPTIONAL_STACKS)[number]

export const PROPERTY_FEATURES = {
	IngestTracking: 'sleeper.ingest.tracker.enabled',
	CompactionTracking: 'sleeper.compaction.tracker.enabled',
} as const

export type PropertyFeatureName = keyof typeof PROPERTY_FEATURES

export type FeatureName = OptionalStackName | PropertyFeatureName

export type FeatureIcon = ComponentType<IconProps>

export const FEATURE_GROUPS = [
	'Ingest',
	'Bulk Import',
	'Bulk Export',
	'Query',
	'Data Maintenance',
	'REST API',
	'Metrics',
] as const

export type FeatureGroup = (typeof FEATURE_GROUPS)[number]

export interface FeatureInfo {
	key: FeatureName
	name: string
	group: FeatureGroup
	description: string
	icon: FeatureIcon
	path?: string
}

export interface EnableInstruction {
	kind: 'stack' | 'property'
	property: string
	value: string
	link: string
}

export function propertyLink(basePath: string, property: string): string {
	return `${basePath}?filter=${encodeURIComponent(property)}`
}

export function enableInstruction(key: FeatureName): EnableInstruction {
	const property = PROPERTY_FEATURES[key as PropertyFeatureName]

	if (property) {
		return { kind: 'property', property, value: 'true', link: propertyLink('/instance/properties', property) }
	}

	const stacks = 'sleeper.optional.stacks'
	return { kind: 'stack', property: stacks, value: key, link: propertyLink('/instance/properties', stacks) }
}

export const FEATURES: FeatureInfo[] = [
	{
		key: 'IngestStack',
		name: 'Standard Ingest',
		group: 'Ingest',
		description: 'Ingests files by putting a job on an SQS queue, picked up by ECS tasks that write the data into Sleeper.',
		icon: AmazonElasticContainerService,
	},
	{
		key: 'IngestBatcherStack',
		name: 'Ingest Batcher',
		group: 'Ingest',
		description: 'Collects individual files submitted for ingest and groups them into larger ingest or bulk import jobs.',
		icon: AmazonSimpleQueueService,
		path: '/ingest-batcher',
	},
	{
		key: 'IngestTracking',
		name: 'Ingest Tracking',
		group: 'Ingest',
		description: 'Stores the status of ingest jobs and tasks so you can follow their progress on the Ingest Jobs page.',
		icon: AmazonDynamoDb,
		path: '/ingest-jobs',
	},
	{
		key: 'EmrServerlessBulkImportStack',
		name: 'Bulk Import on EMR Serverless',
		group: 'Bulk Import',
		description: 'Runs bulk import jobs with Spark on EMR Serverless, with no cluster to manage.',
		icon: AmazonEmr,
	},
	{
		key: 'EmrBulkImportStack',
		name: 'Bulk Import on EMR',
		group: 'Bulk Import',
		description: 'Runs bulk import jobs with Spark on an EMR cluster created for the job and torn down afterwards.',
		icon: AmazonEmr,
	},
	{
		key: 'PersistentEmrBulkImportStack',
		name: 'Bulk Import on persistent EMR',
		group: 'Bulk Import',
		description: 'Runs bulk import jobs with Spark on an always-running EMR cluster, which scales up and down on demand.',
		icon: AmazonEmr,
	},
	{
		key: 'EksBulkImportStack',
		name: 'Bulk Import on EKS',
		group: 'Bulk Import',
		description: 'Runs bulk import jobs with Spark on Kubernetes using EKS. Experimental.',
		icon: AmazonElasticKubernetesService,
	},
	{
		key: 'EmrStudioStack',
		name: 'EMR Studio',
		group: 'Bulk Import',
		description: 'Creates an EMR Studio for inspecting and debugging the EMR Serverless application. Only deployed if EMR Serverless bulk import is also enabled.',
		icon: AwsAppStudio,
	},
	{
		key: 'BulkExportStack',
		name: 'Bulk Export',
		group: 'Bulk Export',
		description: 'Exports a whole Sleeper table as Parquet files.',
		icon: AmazonSimpleStorageService,
	},
	{
		key: 'QueryStack',
		name: 'Query',
		group: 'Query',
		description: 'Handles queries submitted via SQS, executed by Lambda, with results written to S3. Powers the Queries page and the query wizard.',
		icon: AwsLambda,
		path: '/queries',
	},
	{
		key: 'WebSocketQueryStack',
		name: 'WebSocket Query',
		group: 'Query',
		description: 'Handles queries over a WebSocket API, so results stream back to the client as they are found.',
		icon: AmazonApiGateway,
	},
	{
		key: 'AthenaStack',
		name: 'Athena SQL',
		group: 'Query',
		description: 'Registers Sleeper tables as an Athena data source so you can run SQL analytics over the data. Experimental.',
		icon: AmazonAthena,
	},
	{
		key: 'KeepLambdaWarmStack',
		name: 'Keep Lambdas Warm',
		group: 'Query',
		description: 'Sends dummy queries on a schedule so query lambdas stay warm and real queries do not pay a cold start.',
		icon: AmazonEventBridge,
	},
	{
		key: 'CompactionStack',
		name: 'Compaction',
		group: 'Data Maintenance',
		description: 'Merges small files into larger ones on ECS tasks, keeping queries fast.',
		icon: AwsFargate,
	},
	{
		key: 'CompactionTracking',
		name: 'Compaction Tracking',
		group: 'Data Maintenance',
		description: 'Stores the status of compaction jobs and tasks so their progress can be reported.',
		icon: AmazonDynamoDb,
	},
	{
		key: 'GarbageCollectorStack',
		name: 'Garbage Collection',
		group: 'Data Maintenance',
		description: 'Deletes files left behind by compaction once they are old enough that no query can still be reading them.',
		icon: GarbageCollectionIcon,
	},
	{
		key: 'PartitionSplittingStack',
		name: 'Partition Splitting',
		group: 'Data Maintenance',
		description: 'Splits partitions when they hold too much data, so tables keep scaling as they grow.',
		icon: PartitionSplittingIcon,
	},
	{
		key: 'RestApiStack',
		name: 'REST API',
		group: 'REST API',
		description: 'Exposes a REST API for interacting with the instance over HTTPS. Currently only the add-table endpoint is available.',
		icon: AmazonApiGateway,
	},
	{
		key: 'DashboardStack',
		name: 'CloudWatch Dashboard',
		group: 'Metrics',
		description: 'Creates a CloudWatch dashboard showing the metrics recorded for this instance.',
		icon: AmazonCloudWatch,
	},
	{
		key: 'TableMetricsStack',
		name: 'Table Metrics',
		group: 'Metrics',
		description: 'Publishes CloudWatch metrics such as row count, file count and partition count for each table over time.',
		icon: AmazonCloudWatch,
		path: '/data',
	},
]
