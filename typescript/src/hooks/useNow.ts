import { useEffect, useState } from 'react'

const DEFAULT_INTERVAL_MS = 1000

export function useNow(active: boolean, intervalMs: number = DEFAULT_INTERVAL_MS): number {
	const [now, setNow] = useState(() => Date.now())
	useEffect(() => {
		if (!active) return
		setNow(Date.now())
		const id = setInterval(() => setNow(Date.now()), intervalMs)
		return () => clearInterval(id)
	}, [active, intervalMs])
	return now
}
