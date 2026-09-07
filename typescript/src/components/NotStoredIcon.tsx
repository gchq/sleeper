
export default function NotStoredIcon({ width, height }: { width: number; height: number }) {
	return (
		<svg width={width} height={height} viewBox="0 0 24 24" fill="none" aria-hidden="true">
			<circle cx="12" cy="12" r="9" stroke="#9ca3af" strokeWidth="2" />
			<line x1="6" y1="18" x2="18" y2="6" stroke="#9ca3af" strokeWidth="2" />
		</svg>
	)
}
