import { ICON_INK, type IconProps } from './icon'

export default function PartitionSplittingIcon({ width = 32, height = 32 }: IconProps) {
	return (
		<svg width={width} height={height} viewBox="0 0 24 24" fill="none" aria-hidden="true">
			<path
				d="M12 3v6M12 9c0 2.5-5 2-5 5v1M12 9c0 2.5 5 2 5 5v1"
				stroke={ICON_INK}
				strokeWidth="1.6"
				strokeLinecap="round"
				strokeLinejoin="round"
			/>
			<rect x="4" y="16" width="6" height="5" rx="1" stroke={ICON_INK} strokeWidth="1.6" />
			<rect x="14" y="16" width="6" height="5" rx="1" stroke={ICON_INK} strokeWidth="1.6" />
		</svg>
	)
}
