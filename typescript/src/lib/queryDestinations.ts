import type { ComponentType } from 'react'
import { AmazonApiGatewayEndpoint, AmazonSimpleQueueServiceMessage, Documents } from '@aws-icons/react/resource'
import NotStoredIcon from '../components/NotStoredIcon'
import type { ResultsLocation } from './queryResults'

export interface Destination {
	kind: 's3' | 'sqs' | 'websocket' | 'none'
	label: string
	icon: ComponentType<{ width: number; height: number }>
	matches: (type: string) => boolean
	typeLabels: Record<string, string>
}

const NOT_STORED: Destination = {
	kind: 'none',
	label: 'Not stored',
	icon: NotStoredIcon,
	matches: () => true,
	typeLabels: { destination: 'Destination' },
}

const DESTINATIONS: Destination[] = [
	{
		kind: 's3',
		label: 'Written to S3',
		icon: Documents,
		matches: (type) => type === 's3',
		typeLabels: { s3: 'S3' },
	},
	{
		kind: 'sqs',
		label: 'Sent to the results SQS queue',
		icon: AmazonSimpleQueueServiceMessage,
		matches: (type) => type === 'sqs',
		typeLabels: { sqs: 'SQS queue' },
	},
	{
		kind: 'websocket',
		label: 'Sent over a WebSocket connection',
		icon: AmazonApiGatewayEndpoint,
		matches: (type) => type.startsWith('websocket'),
		typeLabels: {
			'websocket-endpoint': 'WebSocket endpoint',
			'websocket-connection-id': 'WebSocket connection',
		},
	},
	NOT_STORED,
]

function destinationFor(type: string): Destination {
	return DESTINATIONS.find(destination => destination.matches(type)) ?? NOT_STORED
}

export function destinationTypeLabel(type: string): string {
	return destinationFor(type).typeLabels[type] ?? type
}

export function destinationsFor(locations: ResultsLocation[]): Destination[] {
	const destinations: Destination[] = []
	for (const location of locations ?? []) {
		const destination = destinationFor(location.type)
		if (!destinations.includes(destination)) destinations.push(destination)
	}
	return destinations
}
