import { useEffect, useState } from 'react'

interface UseApiResult<T> {
	data: T | null
	loading: boolean
	error: string | null
}

export function useApi<T>(url: string, refreshInterval: number = 60, timeout?: number): UseApiResult<T> {
	if (refreshInterval < 0 || !Number.isInteger(refreshInterval)) {
		throw new Error('refreshInterval must be a non-negative integer')
	}

	// If set, use timeout prop value
	// If not, use 60% of refreshInterval prop value
	// Default to 60 secs otherwise
	const resolvedTimeout = timeout ?? (refreshInterval > 0 ? Math.max(1, Math.floor(refreshInterval * 2 / 3)) : 60)

	const [data, setData] = useState<T | null>(null)
	const [error, setError] = useState<string | null>(null)
	const [loading, setLoading] = useState(true)

	useEffect(() => {
		setData(null)
		setError(null)

		let cancelled = false
		const controller = new AbortController()

		function load() {
			setLoading(true)
			const signal = AbortSignal.any([
				controller.signal,
				AbortSignal.timeout(resolvedTimeout * 1000)
			])

			fetch('/api' + url, { signal })
				.then((response) => {
					if (!response.ok) {
						throw new Error(`HTTP error: ${response.status}`)
					}
					return response.json()
				})
				.then((json: T) => {
					if (!cancelled) {
						setData(json)
						setError(null)
						setLoading(false)
					}
				})
				.catch((err: Error) => {
					if (!cancelled) {
						setError(err.name === 'TimeoutError' ? `Request timed out after ${resolvedTimeout}s` : err.message)
						setLoading(false)
					}
				})
		}

		load()

		if (refreshInterval === 0) return () => {
			cancelled = true
			controller.abort()
		}

		const id = setInterval(load, refreshInterval * 1000)
		return () => {
			cancelled = true
			controller.abort()
			clearInterval(id)
		}
	}, [url, refreshInterval, resolvedTimeout])

	return { data, loading, error }
}
