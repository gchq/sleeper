import type { IconProps } from './icon'

export default function HelpIcon({ width = 18, height = 18 }: IconProps) {
	return (
		<svg width={width} height={height} viewBox="0 0 24 24" fill="none" aria-hidden="true">
			<circle cx="12" cy="12" r="9" stroke="currentColor" strokeWidth="1.7" />
			<path
				d="M9.5 9.3a2.6 2.6 0 1 1 3.3 2.5c-.5.2-.8.6-.8 1.1v.6"
				stroke="currentColor"
				strokeWidth="1.7"
				strokeLinecap="round"
			/>
			<circle cx="12" cy="16.5" r="1" fill="currentColor" />
		</svg>
	)
}