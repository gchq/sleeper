import { useCallback, useEffect, useRef, useState } from 'react'
import { apiUrl } from '../lib/api'

interface UseApiResult<T> {
	data: T | null
	loading: boolean
	error: string | null
	reload: () => void
	refreshInterval: number
	nextReloadAt: number | null
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
	const [nextReloadAt, setNextReloadAt] = useState<number | null>(null)
	const controllerRef = useRef<AbortController | null>(null)

	const load = useCallback(() => {
		// Abort any request already in flight (interval tick or previous reload)
		controllerRef.current?.abort()
		const controller = new AbortController()
		controllerRef.current = controller

		setNextReloadAt(refreshInterval > 0 ? Date.now() + refreshInterval * 1000 : null)
		setLoading(true)
		const signal = AbortSignal.any([
			controller.signal,
			AbortSignal.timeout(resolvedTimeout * 1000)
		])

		fetch(apiUrl(url), { signal })
			.then((response) => {
				if (!response.ok) {
					throw new Error(`HTTP error: ${response.status}`)
				}
				return response.json()
			})
			.then((json: T) => {
				if (!controller.signal.aborted) {
					setData(json)
					setError(null)
					setLoading(false)
				}
			})
			.catch((err: Error) => {
				if (!controller.signal.aborted) {
					setError(err.name === 'TimeoutError' ? `Request timed out after ${resolvedTimeout}s` : err.message)
					setLoading(false)
				}
			})
	}, [url, resolvedTimeout, refreshInterval])

	useEffect(() => {
		setData(null)
		setError(null)
		load()

		if (refreshInterval === 0) return () => controllerRef.current?.abort()

		const id = setInterval(load, refreshInterval * 1000)
		return () => {
			controllerRef.current?.abort()
			clearInterval(id)
		}
	}, [load, refreshInterval])

	return { data, loading, error, reload: load, refreshInterval, nextReloadAt }
}
