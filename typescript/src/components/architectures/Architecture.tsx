import { useLayoutEffect, useRef, useState } from 'react'
import type { AwsIconComponent } from '@aws-icons/react'
import ReloadButton from '../ReloadButton'
import ResourceDetailModal from './ResourceDetailModal'
import { useApi } from '../../hooks/useApi'
import { useInstance } from '../../contexts/InstanceContext'
import './Architecture.css'

interface ArchitectureResponse {
	resources: Record<string, ArchitectureResource>
}

export interface ArchitectureResource {
	type: string
	name: string
	arn: string | null
	url: string | null
	logGroup: string | null
	consoleUrl: string | null
	status: ResourceStatus
	detail: string | null
}

export type ResourceStatus = 'ok' | 'warning' | 'error' | 'unknown'

export interface NodeConfig {
	shortTitle?: string
	longTitle?: string
	col: number
	row: number
	icon: AwsIconComponent
}

export interface Edge<ResourceKey extends string = string> {
	from: ResourceKey
	to: ResourceKey
	errorPath?: boolean
}

const STATUS_LABEL: Record<ResourceStatus, string> = {
	ok: 'Healthy',
	warning: 'Warning',
	error: 'Error',
	unknown: 'Unknown',
}

function statusTitle(resource: ArchitectureResource): string {
	const base = STATUS_LABEL[resource.status]
	return resource.detail ? `${base} — ${resource.detail}` : base
}

interface ArchitectureProps<ResourceKey extends string = string> {
	url: string
	nodes: Record<ResourceKey, NodeConfig>
	edges: Edge<ResourceKey>[]
	title?: string
	initiallyCollapsed?: boolean
}

/**
 * Generic collapsible diagram of the AWS resources that implement a Sleeper component.
 */
export default function Architecture({
	url,
	nodes,
	edges,
	title = Object.keys(nodes).length + ' AWS Resources',
	initiallyCollapsed = false,
}: ArchitectureProps) {
	const { region } = useInstance()
	const [selectedKey, setSelectedKey] = useState<string | null>(null)
	const [collapsed, setCollapsed] = useState(initiallyCollapsed)

	const { data, loading, error, reload, nextReloadAt } = useApi<ArchitectureResponse>(
		url,
		collapsed ? 0 : 60,
	)

	const resources = data?.resources ?? {}
	const errorCount = Object.values(resources).filter(r => r.status === 'error').length
	const warningCount = Object.values(resources).filter(r => r.status === 'warning').length
	const hasError = errorCount > 0 || warningCount > 0
	const selected = selectedKey ? resources[selectedKey] : null

	return (
		<section
			className={`arch${hasError ? ' has-error' : ''}${collapsed ? ' collapsed' : ''}`}
			aria-label={title}
		>
			<div
				className="arch-header"
				role="button"
				tabIndex={0}
				onClick={() => setCollapsed((c) => !c)}
				onKeyDown={(e) => {
					if (e.key === 'Enter' || e.key === ' ') {
						e.preventDefault()
						setCollapsed((c) => !c)
					}
				}}
				aria-expanded={!collapsed}
				aria-label={collapsed ? `Expand ${title}` : `Collapse ${title}`}
			>
				<svg className="arch-chevron" viewBox="0 0 12 12" aria-hidden="true">
					<path d="M4 2 L9 6 L4 10 Z" fill="currentColor" />
				</svg>
				<span className="arch-title">{title}</span>
				{errorCount > 0 && (
					<span className="arch-badge error">
						{errorCount} error{errorCount === 1 ? '' : 's'}
					</span>
				)}
				{warningCount > 0 && (
					<span className="arch-badge warning">
						{warningCount} warning{warningCount === 1 ? '' : 's'}
					</span>
				)}
				<div className="arch-spacer" />
				{!collapsed && (
					<span onClick={(e) => e.stopPropagation()}>
						<ReloadButton onReload={reload} loading={loading} nextReloadAt={nextReloadAt} />
					</span>
				)}
			</div>

			{!collapsed && (
				<>
					{error && <p className="error">Failed to load {title}: {error}</p>}

					{!error && Object.keys(resources).length > 0 && (
						<Graph resources={resources} nodes={nodes} edges={edges} onSelect={setSelectedKey} />
					)}
				</>
			)}

			{selected && (
				<ResourceDetailModal
					resource={selected}
					icon={selectedKey ? nodes[selectedKey].icon : null}
					region={region}
					onClose={() => setSelectedKey(null)}
				/>
			)}
		</section>
	)
}

interface Rect {
	x: number
	y: number
	w: number
	h: number
}

function Graph({
	resources,
	nodes,
	edges,
	onSelect,
}: {
	resources: Record<string, ArchitectureResource>
	nodes: Record<string, NodeConfig>
	edges: Edge[]
	onSelect: (key: string) => void
}) {
	const resourceKeys = new Set(Object.keys(resources))
	const edgesToDisplay = edges.filter(e => resourceKeys.has(e.from) && resourceKeys.has(e.to))
	const affectedResources = downstreamOfErrors(resources, edgesToDisplay)

	const gridRef = useRef<HTMLDivElement>(null)
	const nodeRefs = useRef(new Map<string, HTMLElement>())
	const [rects, setRects] = useState<Record<string, Rect>>({})
	const [size, setSize] = useState({ width: 0, height: 0 })

	// Measure node positions relative to the grid so the SVG overlay can draw connector lines.
	useLayoutEffect(() => {
		const grid = gridRef.current
		if (!grid) return

		const measure = () => {
			const next: Record<string, Rect> = {}
			let width = 0
			let height = 0
			const gridRect = grid.getBoundingClientRect()
			nodeRefs.current.forEach((el, key) => {
				const r = el.getBoundingClientRect()
				const rect = { x: r.left - gridRect.left, y: r.top - gridRect.top, w: r.width, h: r.height }
				next[key] = rect
				width = Math.max(width, rect.x + rect.w)
				height = Math.max(height, rect.y + rect.h)
			})
			setRects(next)
			setSize({ width, height })
		}

		measure()
		const observer = new ResizeObserver(measure)
		observer.observe(grid)
		nodeRefs.current.forEach((el) => observer.observe(el))
		return () => observer.disconnect()
	}, [resources])

	const setNodeRef = (key: string) => (el: HTMLElement | null) => {
		if (el) nodeRefs.current.set(key, el)
		else nodeRefs.current.delete(key)
	}

	return (
		<div className="arch-graph" ref={gridRef}>
			<svg className="arch-edges" width={size.width} height={size.height} aria-hidden="true">
				<defs>
					<marker id="arch-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
						<path d="M0,0 L8,4 L0,8 Z" fill="#9ca3af" />
					</marker>
					<marker id="arch-arrow-fail" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
						<path d="M0,0 L8,4 L0,8 Z" fill="#dc2626" />
					</marker>
				</defs>

				{edgesToDisplay.map((edge) => {
					const from = rects[edge.from]
					const to = rects[edge.to]
					if (!from || !to) return null
					const start = perimeterPoint(from, center(to))
					const end = perimeterPoint(to, center(from))
					const errorPath = edge.errorPath ?? false
					const dimmed = affectedResources.has(edge.to)
					return (
						<line
							key={`${edge.from}-${edge.to}`}
							x1={start.x}
							y1={start.y}
							x2={end.x}
							y2={end.y}
							className={`arch-edge${errorPath ? ' failure' : ''}${dimmed ? ' dimmed' : ''}`}
							markerEnd={errorPath ? 'url(#arch-arrow-fail)' : 'url(#arch-arrow)'}
						/>
					)
				})}
			</svg>

			{Object.entries(resources).map(([key, resource]) => {
				const node = nodes[key]
				if (!node) return null
				return (
					<div
						key={key}
						className="arch-cell"
						style={{ gridColumn: node.col, gridRow: node.row }}
					>
						<Node
							resource={resource}
							node={node}
							affected={affectedResources.has(key)}
							onSelect={() => onSelect(key)}
							cardRef={setNodeRef(key)}
						/>
					</div>
				)
			})}
		</div>
	)
}

// Returns the set of resource keys that are downstream of an error, following data-flow edges from
// each node with an error. Failure edges (e.g. to a dead-letter queue) are not treated as dependencies.
function downstreamOfErrors(
	resources: Record<string, ArchitectureResource>,
	edges: Edge[],
): Set<string> {
	const nodesWithErrors = Object.entries(resources)
		.filter(([, r]) => r.status === 'error')
		.map(([key]) => key)

	const affected = new Set<string>()
	const queue = [...nodesWithErrors]
	while (queue.length > 0) {
		const current = queue.shift()
		for (const edge of edges) {
			if (edge.errorPath || edge.from !== current) continue
			if (!affected.has(edge.to)) {
				affected.add(edge.to)
				queue.push(edge.to)
			}
		}
	}
	return affected
}

function center(rect: Rect) {
	return { x: rect.x + rect.w / 2, y: rect.y + rect.h / 2 }
}

// Point on a rect's border in the direction of the target point — used to anchor connector lines.
// `gap` pushes the point outward along the direction so lines don't touch the icon border.
function perimeterPoint(rect: Rect, target: { x: number; y: number }, gap = 6) {
	const cx = rect.x + rect.w / 2
	const cy = rect.y + rect.h / 2
	const dx = target.x - cx
	const dy = target.y - cy
	if (dx === 0 && dy === 0) return { x: cx, y: cy }
	const scale = 1 / Math.max(Math.abs(dx) / (rect.w / 2), Math.abs(dy) / (rect.h / 2))
	const len = Math.hypot(dx, dy)
	return { x: cx + dx * scale + (dx / len) * gap, y: cy + dy * scale + (dy / len) * gap }
}

function Node({
	resource,
	node,
	affected,
	onSelect,
	cardRef,
}: {
	resource: ArchitectureResource
	node: NodeConfig
	affected: boolean
	onSelect: () => void
	cardRef: (el: HTMLElement | null) => void
}) {
	const title = affected ? `${statusTitle(resource)} (affected by an upstream error)` : statusTitle(resource)

	return (
		<div className={`arch-node ${resource.status}${affected ? ' affected' : ''}`}>
			<button
				type="button"
				ref={cardRef}
				className="arch-node-card"
				title={title}
				aria-label={title}
				onClick={onSelect}
			>
				<node.icon width={34} height={34} />
			</button>
			<span className="arch-node-name" title={resource.name}>
				{node.shortTitle || node.longTitle || resource.name}
			</span>
		</div>
	)
}
