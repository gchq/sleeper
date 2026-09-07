import { ICON_INK, type IconProps } from './icon'

export default function GarbageCollectionIcon({ width = 32, height = 32 }: IconProps) {
	return (
		<svg width={width} height={height} viewBox="0 0 24 24" fill="none" aria-hidden="true">
			<path
				d="M4 6h16M9.5 6V4h5v2M6.5 6l1 13.5h9L17.5 6"
				stroke={ICON_INK}
				strokeWidth="1.6"
				strokeLinecap="round"
				strokeLinejoin="round"
			/>
			<path d="M10 9.5v7M14 9.5v7" stroke={ICON_INK} strokeWidth="1.6" strokeLinecap="round" />
		</svg>
	)
}
