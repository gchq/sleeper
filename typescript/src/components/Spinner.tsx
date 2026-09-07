import './Spinner.css'

interface Props {
	size?: number
	label?: string
}

export default function Spinner({ size = 14, label = 'Loading' }: Props) {
	return (
		<svg
			className="spinner"
			role="img"
			aria-label={label}
			width={size}
			height={size}
			viewBox="0 0 16 16"
			fill="none"
			stroke="currentColor"
			strokeWidth="2"
			strokeLinecap="round"
		>
			<title>{label}</title>
			<circle cx="8" cy="8" r="6" opacity="0.25" />
			<path d="M8 2a6 6 0 0 1 6 6" />
		</svg>
	)
}
