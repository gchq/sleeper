
const S3_CONSOLE_BASE = 'https://s3.console.aws.amazon.com/s3/buckets'

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
export function s3ConsoleUrl(path: string, region?: string | null): string | null {
	const parsed = parseS3Path(path)
	if (!parsed) return null
	const params = new URLSearchParams()
	if (parsed.key) params.set('prefix', parsed.key.endsWith('/') ? parsed.key : parsed.key + '/')
	if (region) params.set('region', region)
	const query = params.toString()
	return `${S3_CONSOLE_BASE}/${encodeURIComponent(parsed.bucket)}${query ? '?' + query : ''}`
}

export function s3ConsoleHomeUrl(region?: string | null): string {
	return region ? `${S3_CONSOLE_BASE}?region=${encodeURIComponent(region)}` : S3_CONSOLE_BASE
}