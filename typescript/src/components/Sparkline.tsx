import './Sparkline.css'

export interface SparklinePoint {
	time: number
	value: number | null
}

interface SparklineProps {
	points: SparklinePoint[]
	domain: [number, number]
	color: string
	width?: number
	height?: number
	strokeWidth?: number
	fill?: boolean
	title?: string
}

interface ResolvedPoint {
	time: number
	value: number
}

function computeRuns(points: SparklinePoint[]): ResolvedPoint[][] {
	const runs: ResolvedPoint[][] = []
	let current: ResolvedPoint[] = []
	for (const p of points) {
		if (p.value == null || !Number.isFinite(p.value)) {
			if (current.length > 0) {
				runs.push(current)
				current = []
			}
		} else {
			current.push({ time: p.time, value: p.value as number })
		}
	}
	if (current.length > 0) runs.push(current)
	return runs
}

export default function Sparkline({
	points,
	domain,
	color,
	width = 120,
	height = 32,
	strokeWidth = 1.5,
	fill = true,
	title,
}: SparklineProps) {
	const runs = computeRuns(points)
	const allPoints = runs.flat()

	if (allPoints.length === 0) {
		return (
			<div className="sparkline sparkline-empty" style={{ width, height }} aria-hidden="true">
				<span>—</span>
			</div>
		)
	}

	const [xStart, xEnd] = domain
	const xRange = xEnd - xStart || 1
	const ys = allPoints.map((p) => p.value)
	const yMin = Math.min(...ys)
	const yMax = Math.max(...ys)
	const yRange = yMax - yMin

	const pad = strokeWidth
	const scaleX = (t: number) => pad + ((t - xStart) / xRange) * (width - 2 * pad)
	const flatY = yMin === 0 ? height - pad : height / 2
	const scaleY = yRange === 0
		? () => flatY
		: (y: number) => height - pad - ((y - yMin) / yRange) * (height - 2 * pad)

	const linePaths: string[] = []
	const areaPaths: string[] = []
	const singleDots: ResolvedPoint[] = []
	for (const run of runs) {
		if (run.length < 2) {
			singleDots.push(run[0])
			continue
		}
		const pts = run.map((p) => `${scaleX(p.time).toFixed(2)},${scaleY(p.value).toFixed(2)}`)
		const line = 'M' + pts.join(' L')
		linePaths.push(line)
		if (fill) {
			const first = run[0]
			const last = run[run.length - 1]
			areaPaths.push(`${line} L${scaleX(last.time).toFixed(2)},${height} L${scaleX(first.time).toFixed(2)},${height} Z`)
		}
	}

	const lastPoint = allPoints[allPoints.length - 1]

	return (
		<svg
			className="sparkline"
			width={width}
			height={height}
			viewBox={`0 0 ${width} ${height}`}
			preserveAspectRatio="none"
			role="img"
			aria-label={title}
		>
			{areaPaths.map((d, i) => (
				<path key={`a${i}`} d={d} fill={color} fillOpacity={0.12} />
			))}
			{linePaths.map((d, i) => (
				<path
					key={`l${i}`}
					d={d}
					fill="none"
					stroke={color}
					strokeWidth={strokeWidth}
					strokeLinejoin="round"
					strokeLinecap="round"
					vectorEffect="non-scaling-stroke"
				/>
			))}
			{singleDots.map((p, i) => (
				<circle key={`s${i}`} cx={scaleX(p.time)} cy={scaleY(p.value)} r={1.75} fill={color} />
			))}
			<circle cx={scaleX(lastPoint.time)} cy={scaleY(lastPoint.value)} r={2.5} fill={color} />
		</svg>
	)
}
