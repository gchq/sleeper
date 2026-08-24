import { useCallback, useEffect, useRef, useState } from 'react'

/**
 * Copy-to-clipboard helper that exposes the key of the most recently copied value so a caller can
 * show a transient "Copied" indicator. The flag clears itself after `resetMs`.
 */
export function useCopyToClipboard(resetMs = 500): {
	copiedKey: string | null
	copy: (text: string, key: string) => void
} {
	const [copiedKey, setCopiedKey] = useState<string | null>(null)
	const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

	useEffect(() => () => {
		if (timeoutRef.current) clearTimeout(timeoutRef.current)
	}, [])

	const copy = useCallback((text: string, key: string) => {
		void navigator.clipboard.writeText(text).then(() => {
			setCopiedKey(key)
			if (timeoutRef.current) clearTimeout(timeoutRef.current)
			timeoutRef.current = setTimeout(() => setCopiedKey(null), resetMs)
		})
	}, [resetMs])

	return { copiedKey, copy }
}
