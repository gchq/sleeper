import { useEffect } from 'react'
import type { AwsIconComponent } from '@aws-icons/react'
import { useCopyToClipboard } from '../../hooks/useCopyToClipboard'
import {
	cloudWatchLogsUrl,
	dynamoExploreItemsUrl,
	dynamoMonitoringUrl,
	eventBridgeRuleConsoleUrl,
	lambdaMonitoringUrl,
	parseArn,
	s3ConsoleUrl,
	sqsQueueConsoleUrl,
	type ConsoleLink,
} from '../../lib/aws'
import { ArchitectureResource, ResourceStatus } from './Architecture'
import './ResourceDetailModal.css'

const STATUS_LABEL: Record<ResourceStatus, string> = {
	ok: 'Healthy',
	warning: 'Warning',
	error: 'Error',
	unknown: 'Unknown',
}

interface CopyField {
	label: string
	value: string
}

function copyFields(resource: ArchitectureResource): CopyField[] {
	const fields: CopyField[] = [{ label: 'Name', value: resource.name }]
	if (resource.arn) fields.push({ label: 'ARN', value: resource.arn })
	if (resource.url) fields.push({ label: 'URL', value: resource.url })
	if (resource.logGroup) fields.push({ label: 'Log Group', value: resource.logGroup })
	return fields
}

function consoleLinks(resource: ArchitectureResource, region: string | null): ConsoleLink[] {
	if (!region) return []
	const arn = resource.arn ? parseArn(resource.arn) : null
	const links: ConsoleLink[] = []
	switch (resource.type.toLowerCase()) {
		case 'sqs::queue':
			if (resource.url) links.push({ label: 'SQS Console', url: sqsQueueConsoleUrl(resource.url, region) })
			break

		case 'lambda::function':
			const fn = arn?.resource?.replace(/^function:/, '')
			if (fn) links.push({ label: 'Monitoring', url: lambdaMonitoringUrl(fn, region) })
			if (resource.logGroup) links.push({ label: 'Logs', url: cloudWatchLogsUrl(resource.logGroup, region) })
			break

		case 'dynamodb::table':
			const table = arn?.resource?.replace(/^table\//, '')
			if (table) {
				links.push({ label: 'Monitoring', url: dynamoMonitoringUrl(table, region) })
				links.push({ label: 'Explore Items', url: dynamoExploreItemsUrl(table, region) })
			}
			break

		case 'eventbridge::rule':
			const rule = arn?.resource?.replace(/^rule\//, '')
			if (rule) links.push({ label: 'View Rule', url: eventBridgeRuleConsoleUrl(rule, region) })
			break

		case 's3::bucket':
			const url = s3ConsoleUrl(resource.name, region)
			if (url) links.push({ label: 'View Bucket', url })
			break;
	}
	return links
}

export default function ResourceDetailModal({
	resource,
	icon: Icon,
	region,
	onClose,
}: {
	resource: ArchitectureResource
	icon?: AwsIconComponent | null
	region: string | null
	onClose: () => void
}) {
	const { copiedKey, copy } = useCopyToClipboard()

	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onClose()
		}
		window.addEventListener('keydown', onKey)
		return () => window.removeEventListener('keydown', onKey)
	}, [onClose])

	const fields = copyFields(resource)
	const links = consoleLinks(resource, region)

	return (
		<div className="modal-backdrop" onClick={onClose}>
			<div className="modal resource-modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
				<div className="resource-modal-head">
					{Icon && (
						<span className="resource-modal-icon" aria-hidden="true">
							<Icon width={36} height={36} />
						</span>
					)}
					<div className="resource-modal-heading">
						<h3 className="modal-title">{resource.name}</h3>
						<span className="resource-modal-type">{resource.type}</span>
					</div>
					<span className={`resource-modal-status ${resource.status}`}>
						{STATUS_LABEL[resource.status]}
					</span>
				</div>

				{resource.detail && <p className={`resource-modal-detail ${resource.status}`}>{resource.detail}</p>}

				<div className="resource-modal-fields">
					{fields.map((field) => (
						<div className="resource-modal-field" key={field.label}>
							<span className="resource-modal-field-label">{field.label}</span>
							<div className="resource-modal-field-row">
								<code className="resource-modal-field-value">{field.value}</code>
								<button
									type="button"
									className="btn resource-modal-copy"
									onClick={() => copy(field.value, field.label)}
									aria-label={`Copy ${field.label}`}
								>
									{copiedKey === field.label ? 'Copied' : 'Copy'}
								</button>
							</div>
						</div>
					))}
				</div>

				<div className="modal-actions">
					<button type="button" className="btn" onClick={onClose}>
						Close
					</button>

					{links.length > 0 && (
						<div className="resource-modal-links">
							{links.map((link) => (
								<a
									key={link.label}
									className="btn btn-primary resource-modal-link"
									href={link.url}
									target="_blank"
									rel="noopener noreferrer"
								>
									{link.label}
								</a>
							))}
						</div>
					)}
				</div>
			</div>
		</div>
	)
}
