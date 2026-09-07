import type { ChangeEvent, MouseEvent } from 'react'
import { useEffect, useState } from 'react'
import { NavLink, useLocation, useMatch, useNavigate } from 'react-router-dom'
import type { InstanceFeatures } from '../contexts/InstanceContext'
import { useInstance } from '../contexts/InstanceContext'
import { clearLastTable, readLastTable, writeLastTable } from '../lib/lastTable'
import './Sidebar.css'

interface TablePage {
	key: string
	label: string
	shortLabel: string
	feature?: keyof InstanceFeatures
}

const TABLE_PAGES: TablePage[] = [
	{ key: 'properties', label: 'Table Properties', shortLabel: 'TP' },
	{ key: 'ingest-batcher', label: 'Ingest Batcher', shortLabel: 'IB', feature: 'IngestBatcherStack' },
	{ key: 'ingest-jobs', label: 'Ingest Jobs', shortLabel: 'IJ', feature: 'IngestTracking' },
	{ key: 'queries', label: 'Queries', shortLabel: 'Q', feature: 'QueryStack' },
]

const COLLAPSED_STORAGE_KEY = 'sleeper-sidebar-collapsed'

function readCollapsed(): boolean {
	try {
		return window.localStorage.getItem(COLLAPSED_STORAGE_KEY) === '1'
	} catch {
		return false
	}
}

export default function Sidebar() {
	const navigate = useNavigate()
	const location = useLocation()
	const [collapsed, setCollapsed] = useState<boolean>(readCollapsed)
	const { tables, loading, error, version, instanceId, features } = useInstance()

	const [lastTableId, setLastTableId] = useState<string | null>(readLastTable)
	const tablePageMatch = useMatch('/tables/:tableId/:tablePage')
	const routeTableId = tablePageMatch?.params.tableId
	const selectedTableId = routeTableId ?? lastTableId

	useEffect(() => {
		try {
			window.localStorage.setItem(COLLAPSED_STORAGE_KEY, collapsed ? '1' : '0')
		} catch {
			// Ignore localStorage failures (e.g. private mode or quota exceeded).
		}
	}, [collapsed])

	useEffect(() => {
		if (!routeTableId || routeTableId === lastTableId) return
		setLastTableId(routeTableId)
		writeLastTable(routeTableId)
	}, [routeTableId, lastTableId])

	useEffect(() => {
		if (!tables || !lastTableId) return
		if (tables.some((t) => t.tableUniqueId === lastTableId)) return
		setLastTableId(null)
		clearLastTable()
	}, [tables, lastTableId])

	function onTableChange(e: ChangeEvent<HTMLSelectElement>) {
		const id = e.target.value
		if (!id) return
		if (tablePageMatch) {
			return navigate(`/tables/${encodeURIComponent(id)}/${tablePageMatch.params.tablePage}${location.search}`)
		}

		setLastTableId(id)
		writeLastTable(id)
	}

	function onDisabledLinkClick(e: MouseEvent<HTMLAnchorElement>) {
		e.preventDefault()
	}

	return (
		<aside className={collapsed ? 'sidebar sidebar-collapsed' : 'sidebar'}>
			<div className="sidebar-header">
				{!collapsed && (
					<h1 className="sidebar-title">
						Sleeper
						{version && <span className="sidebar-version" title={"v" + version}>v{version}</span>}
					</h1>
				)}
				<button
					type="button"
					className="sidebar-toggle"
					onClick={() => setCollapsed((c) => !c)}
					aria-label={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
					aria-expanded={!collapsed}
					title={collapsed ? 'Expand sidebar' : 'Collapse sidebar'}
				>
					{collapsed ? '›' : '‹'}
				</button>
			</div>

			{!collapsed && instanceId && (
				<NavLink
					to="/"
					end
					className={({ isActive }) => (isActive ? 'sidebar-instance active' : 'sidebar-instance')}
					title={instanceId + ' — Home'}
				>
					<span className="sidebar-instance-label">Instance</span>
					<span className="sidebar-instance-id">{instanceId}</span>
				</NavLink>
			)}

			<nav className="sidebar-nav" aria-label="Main navigation">
				<div className="sidebar-section">
					{!collapsed && <div className="sidebar-section-label">System</div>}
					{collapsed && (
						<NavLink
							to="/"
							end
							className={({ isActive }) => (isActive ? 'sidebar-link active' : 'sidebar-link')}
							title="Home"
						>
							<span className="sidebar-link-icon" aria-hidden="true">H</span>
						</NavLink>
					)}
					<NavLink
						to="/instance/properties"
						className={({ isActive }) => (isActive ? 'sidebar-link active' : 'sidebar-link')}
						title="Instance Properties"
					>
						<span className="sidebar-link-icon" aria-hidden="true">IP</span>
						{!collapsed && <span className="sidebar-link-label">Instance Properties</span>}
					</NavLink>
					<NavLink
						to="/data"
						className={({ isActive }) => (isActive ? 'sidebar-link active' : 'sidebar-link')}
						title="Data"
					>
						<span className="sidebar-link-icon" aria-hidden="true">D</span>
						{!collapsed && <span className="sidebar-link-label">Data</span>}
					</NavLink>
					<NavLink
						to="/tables"
						end
						className={({ isActive }) => (isActive ? 'sidebar-link active' : 'sidebar-link')}
						title="Tables"
					>
						<span className="sidebar-link-icon" aria-hidden="true">T</span>
						{!collapsed && <span className="sidebar-link-label">Tables</span>}
					</NavLink>
					{features?.IngestBatcherStack && (
						<NavLink
							to="/ingest-batcher"
							end
							className={({ isActive }) => (isActive ? 'sidebar-link active' : 'sidebar-link')}
							title="Ingest Batcher"
						>
							<span className="sidebar-link-icon" aria-hidden="true">IB</span>
							{!collapsed && <span className="sidebar-link-label">Ingest Batcher</span>}
						</NavLink>
					)}
					{features?.IngestTracking && (
						<NavLink
							to="/ingest-jobs"
							end
							className={({ isActive }) => (isActive ? 'sidebar-link active' : 'sidebar-link')}
							title="Ingest Jobs"
						>
							<span className="sidebar-link-icon" aria-hidden="true">IJ</span>
							{!collapsed && <span className="sidebar-link-label">Ingest Jobs</span>}
						</NavLink>
					)}
					{features?.QueryStack && (
						<NavLink
							to="/queries"
							end
							className={({ isActive }) => (isActive ? 'sidebar-link active' : 'sidebar-link')}
							title="Queries"
						>
							<span className="sidebar-link-icon" aria-hidden="true">Q</span>
							{!collapsed && <span className="sidebar-link-label">Queries</span>}
						</NavLink>
					)}
				</div>

				<div className="sidebar-section">
					{!collapsed && <div className="sidebar-section-label">Table</div>}
					{!collapsed && (
						<div className="sidebar-table-select">
							{error ? (
								<span className="sidebar-error" title={error}>
									Failed to load tables
								</span>
							) : (
								<select
									value={selectedTableId ?? ''}
									onChange={onTableChange}
									disabled={loading && !tables}
									aria-label="Select a table"
								>
									<option value="" disabled={!!selectedTableId}>
										{loading && !tables
											? 'Loading tables…'
											: tables && tables.length === 0
												? 'No tables available'
												: 'Select a table…'}
									</option>
									{tables?.map((t) => (
										<option key={t.tableUniqueId} value={t.tableUniqueId}>
											{t.tableName}
											{!t.online ? ' (offline)' : ''}
										</option>
									))}
								</select>
							)}
						</div>
					)}

					{TABLE_PAGES.filter((p) => !p.feature || features?.[p.feature]).map((p) => {
						const disabled = !selectedTableId
						const to = disabled ? '#' : `/tables/${encodeURIComponent(selectedTableId)}/${p.key}`
						return (
							<NavLink
								key={p.key}
								to={to}
								end
								className={({ isActive }) => {
									const classes = ['sidebar-link']
									if (isActive && !disabled) classes.push('active')
									if (disabled) classes.push('disabled')
									return classes.join(' ')
								}}
								title={p.label}
								onClick={disabled ? onDisabledLinkClick : undefined}
								aria-disabled={disabled}
								tabIndex={disabled ? -1 : undefined}
							>
								<span className="sidebar-link-icon" aria-hidden="true">
									{p.shortLabel}
								</span>
								{!collapsed && <span className="sidebar-link-label">{p.label}</span>}
							</NavLink>
						)
					})}
				</div>
			</nav>
		</aside>
	)
}
