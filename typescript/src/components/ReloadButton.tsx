import { useEffect, useState } from 'react'
import './ReloadButton.css'

interface Props {
	onReload: () => void
	loading: boolean
	label?: string
	nextReloadAt?: number | null
	className?: string
}

export default function ReloadButton({ onReload, loading, label, nextReloadAt, className }: Props) {
	const secondsLeft = useCountdown(nextReloadAt, loading)

	return (
		<button
			type="button"
			className={['btn', 'reload-btn', className].filter(Boolean).join(' ')}
			onClick={onReload}
			disabled={loading}
			aria-label={label}
			aria-busy={loading}
			title={label}
		>
			<svg
				className={loading ? 'reload-icon spinning' : 'reload-icon'}
				aria-hidden="true"
				width="15"
				height="15"
				viewBox="0 0 16 16"
				fill="none"
				stroke="currentColor"
				strokeWidth="1.5"
				strokeLinecap="round"
				strokeLinejoin="round"
			>
				<path d="M13.5 8a5.5 5.5 0 1 1-1.6-3.9" />
				<path d="M13.5 2v3h-3" />
			</svg>
			{label && <span className="reload-btn-label">{label}</span>}
			{!loading && secondsLeft !== null && (
				<span className="reload-btn-countdown" aria-hidden="true">
					{`${secondsLeft}s`}
				</span>
			)}
		</button>
	)
}

function useCountdown(nextReloadAt: number | null | undefined, loading: boolean): number | null {
	const [secondsLeft, setSecondsLeft] = useState<number | null>(null)

	useEffect(() => {
		if (nextReloadAt == null) {
			setSecondsLeft(null)
			return
		}
		if (loading) {
			setSecondsLeft(0)
			return
		}
		const tick = () => {
			const remainingMs = nextReloadAt - Date.now()
			setSecondsLeft(Math.max(0, Math.ceil(remainingMs / 1000)))
		}
		tick()
		const id = setInterval(tick, 1000)
		return () => clearInterval(id)
	}, [nextReloadAt, loading])

	return secondsLeft
}