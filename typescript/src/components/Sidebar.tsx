import type { ChangeEvent, MouseEvent } from 'react'
import { useEffect, useState } from 'react'
import { NavLink, useNavigate, useParams } from 'react-router-dom'
import { useTablesList } from '../contexts/TablesContext'
import './Sidebar.css'

interface TablePage {
	key: string
	label: string
	shortLabel: string
}

const TABLE_PAGES: TablePage[] = [
	{ key: 'properties', label: 'Table Properties', shortLabel: 'TP' },
]

const DEFAULT_TABLE_PAGE = TABLE_PAGES[0].key
const COLLAPSED_STORAGE_KEY = 'sleeper-sidebar-collapsed'
const LAST_TABLE_STORAGE_KEY = 'sleeper-sidebar-last-table'

function readCollapsed(): boolean {
	try {
		return window.localStorage.getItem(COLLAPSED_STORAGE_KEY) === '1'
	} catch {
		return false
	}
}

function readLastTable(): string | null {
	try {
		return window.localStorage.getItem(LAST_TABLE_STORAGE_KEY)
	} catch {
		return null
	}
}

export default function Sidebar() {
	const [collapsed, setCollapsed] = useState<boolean>(readCollapsed)
	const [lastTableId, setLastTableId] = useState<string | null>(readLastTable)
	const { tables, loading: tablesLoading, error: tablesError } = useTablesList()
	const { tableId: routeTableId } = useParams<{ tableId?: string }>()
	const navigate = useNavigate()
	const selectedTableId = routeTableId ?? lastTableId

	useEffect(() => {
		try {
			window.localStorage.setItem(COLLAPSED_STORAGE_KEY, collapsed ? '1' : '0')
		} catch {
			// ignore quota / privacy-mode failures
		}
	}, [collapsed])

	useEffect(() => {
		if (!routeTableId || routeTableId === lastTableId) return
		setLastTableId(routeTableId)
		try {
			window.localStorage.setItem(LAST_TABLE_STORAGE_KEY, routeTableId)
		} catch {
			// ignore
		}
	}, [routeTableId, lastTableId])

	useEffect(() => {
		if (!tables || !lastTableId) return
		if (tables.some((t) => t.tableUniqueId === lastTableId)) return
		setLastTableId(null)
		try {
			window.localStorage.removeItem(LAST_TABLE_STORAGE_KEY)
		} catch {
			// ignore
		}
	}, [tables, lastTableId])

	function onTableChange(e: ChangeEvent<HTMLSelectElement>) {
		const id = e.target.value
		if (!id) return
		navigate(`/tables/${encodeURIComponent(id)}/${DEFAULT_TABLE_PAGE}`)
	}

	function onDisabledLinkClick(e: MouseEvent<HTMLAnchorElement>) {
		e.preventDefault()
	}

	return (
		<aside className={collapsed ? 'sidebar sidebar-collapsed' : 'sidebar'}>
			<div className="sidebar-header">
				{!collapsed && <h1 className="sidebar-title">Sleeper</h1>}
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

			<nav className="sidebar-nav" aria-label="Main navigation">
				<div className="sidebar-section">
					{!collapsed && <div className="sidebar-section-label">System</div>}
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
				</div>

				<div className="sidebar-section">
					{!collapsed && <div className="sidebar-section-label">Table</div>}
					{!collapsed && (
						<div className="sidebar-table-select">
							{tablesError ? (
								<span className="sidebar-error" title={tablesError}>
									Failed to load tables
								</span>
							) : (
								<select
									value={selectedTableId ?? ''}
									onChange={onTableChange}
									disabled={tablesLoading && !tables}
									aria-label="Select a table"
								>
									<option value="" disabled={!!selectedTableId}>
										{tablesLoading && !tables
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
					{TABLE_PAGES.map((p) => {
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
								title={disabled ? `${p.label} (select a table first)` : p.label}
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
