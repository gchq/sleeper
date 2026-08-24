
/**
 * Parses an S3 path in s3://, s3a:// or bucket/key form into its bucket and key.
 * Returns null if no bucket can be determined.
 */
export function parseS3Path(path: string): { bucket: string; key: string } | null {
	const withoutScheme = path.replace(/^s3a?:\/\//, '')
	const firstSlash = withoutScheme.indexOf('/')
	const bucket = firstSlash === -1 ? withoutScheme : withoutScheme.slice(0, firstSlash)
	if (!bucket) return null
	const key = firstSlash === -1 ? '' : withoutScheme.slice(firstSlash + 1)
	return { bucket, key }
}

/**
 * Builds a link to the S3 web console that browses a bucket/prefix as a folder listing. The key is
 * used as the console `prefix` with a trailing slash — the console requires this to list a folder;
 * without it, it treats the value as an object key and shows an "object not found" page. Accepts
 * s3://, s3a:// or bucket/key forms. Returns null if no bucket can be determined.
 *
 * @param path   the S3 path (bucket or bucket/prefix)
 * @param region optional AWS region, added as a query param when known
 */
export function s3ConsoleUrl(path: string, region: string): string | null {
	const parsed = parseS3Path(path)
	if (!parsed) return null
	const params = new URLSearchParams()
	if (parsed.key) params.set('prefix', parsed.key.endsWith('/') ? parsed.key : parsed.key + '/')
	if (region) params.set('region', region)
	const query = params.toString()
	return `${consoleHost(region)}/s3/buckets/${encodeURIComponent(parsed.bucket)}${query ? '?' + query : ''}`
}

export function s3ConsoleHomeUrl(region: string): string {
	return `${consoleHost(region)}/s3`
}

export interface ConsoleLink {
	label: string
	url: string
}

/**
 * Parses an ARN into its component parts. Returns null if the value is not a 6+ segment ARN.
 * ARN form: arn:partition:service:region:account-id:resource
 */
export function parseArn(arn: string): { partition: string; service: string; region: string; account: string; resource: string } | null {
	const parts = arn.split(':')
	if (parts.length < 6 || parts[0] !== 'arn') return null
	return {
		partition: parts[1],
		service: parts[2],
		region: parts[3],
		account: parts[4],
		// The resource portion may itself contain colons (e.g. Lambda versions), so re-join the tail.
		resource: parts.slice(5).join(':'),
	}
}

function consoleHost(region: string): string {
	return `https://${region}.console.aws.amazon.com`
}

export function sqsQueueConsoleUrl(queueUrl: string, region: string): string {
	return `${consoleHost(region)}/sqs/v3/home?region=${region}#/queues/${encodeURIComponent(queueUrl)}`
}

export function lambdaMonitoringUrl(functionName: string, region: string): string {
	return `${consoleHost(region)}/lambda/home?region=${region}#/functions/${encodeURIComponent(functionName)}?tab=monitoring`
}

export function cloudWatchLogsUrl(logGroup: string, region: string): string {
	// The CloudWatch console fragment uses a custom encoding: each special character is URL-encoded
	// (e.g. '/' -> '%2F') and then the '%' is replaced with '$25' (so '/' becomes '$252F').
	const encoded = logGroup.replace(/[^a-zA-Z0-9-_.]/g, (ch) => '$25' + ch.charCodeAt(0).toString(16).toUpperCase().padStart(2, '0'))
	return `${consoleHost(region)}/cloudwatch/home?region=${region}#logsV2:log-groups/log-group/${encoded}`
}

export function dynamoMonitoringUrl(tableName: string, region: string): string {
	return `${consoleHost(region)}/dynamodbv2/home?region=${region}#table?name=${encodeURIComponent(tableName)}&tab=monitoring`
}

export function dynamoExploreItemsUrl(tableName: string, region: string): string {
	return `${consoleHost(region)}/dynamodbv2/home?region=${region}#item-explorer?table=${encodeURIComponent(tableName)}`
}

export function eventBridgeRuleConsoleUrl(ruleName: string, region: string): string {
	return `${consoleHost(region)}/events/home?region=${region}#/eventbus/default/rules/${encodeURIComponent(ruleName)}`
}