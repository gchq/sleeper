import { useMemo, useState } from 'react'
import { useApi } from '../hooks/useApi'

interface PropertyDefinition {
	propertyName: string
	description: string
	defaultValue: string
}

type PropertiesResponse = Record<string, PropertyDefinition[]>

export default function InstanceProperties() {
	const { data, error } = useApi<PropertiesResponse>('/sleeper/properties')
	const [search, setSearch] = useState('')
	const [collapsed, setCollapsed] = useState<Set<string>>(new Set())

	const filtered = useMemo(() => {
		if (!data) return null
		const term = search.trim().toLowerCase()
		if (!term) return data
		const result: PropertiesResponse = {}
		for (const [component, props] of Object.entries(data)) {
			const matching = props.filter(
				(p) =>
					p.propertyName.toLowerCase().includes(term) ||
					p.description.toLowerCase().includes(term) ||
					p.defaultValue.toLowerCase().includes(term),
			)
			if (matching.length > 0) {
				result[component] = matching
			}
		}
		return result
	}, [data, search])

	function toggleCollapsed(component: string) {
		setCollapsed((prev) => {
			const next = new Set(prev)
			if (next.has(component)) {
				next.delete(component)
			} else {
				next.add(component)
			}
			return next
		})
	}

	function collapseAll() {
		if (!filtered) return
		setCollapsed(new Set(Object.keys(filtered)))
	}

	function expandAll() {
		setCollapsed(new Set())
	}

	if (error) {
		return (
			<div className="page">
				<h2>Instance Properties</h2>
				<p className="error">Failed to load properties: {error}</p>
			</div>
		)
	}

	if (!filtered) {
		return (
			<div className="page">
				<h2>Instance Properties</h2>
				<p>Loading...</p>
			</div>
		)
	}

	const componentNames = Object.keys(filtered)
	const totalProps = Object.values(filtered).reduce((sum, props) => sum + props.length, 0)

	return (
		<div className="page">
			<h2>Instance Properties</h2>
			<div className="toolbar">
				<input
					className="search-input"
					type="search"
					placeholder="Search properties..."
					value={search}
					onChange={(e) => setSearch(e.target.value)}
					aria-label="Search properties"
				/>
				<span className="result-count">
					{totalProps} {totalProps === 1 ? 'property' : 'properties'} across {componentNames.length}{' '}
					{componentNames.length === 1 ? 'component' : 'components'}
				</span>
				<div className="toolbar-actions">
					<button className="btn-link" onClick={expandAll}>
						Expand all
					</button>
					<button className="btn-link" onClick={collapseAll}>
						Collapse all
					</button>
				</div>
			</div>

			{componentNames.length === 0 && <p className="no-results">No properties match your search.</p>}

			{componentNames.map((component) => {
				const props = filtered[component]
				const isCollapsed = collapsed.has(component)
				return (
					<section key={component} className="component-section">
						<button
							className="component-header"
							onClick={() => toggleCollapsed(component)}
							aria-expanded={!isCollapsed}
						>
							<span className="component-name">{component}</span>
							<span className="component-count">{props.length}</span>
							<span className="collapse-icon">{isCollapsed ? '▶' : '▼'}</span>
						</button>

						{!isCollapsed && (
							<table className="properties-table">
								<thead>
									<tr>
										<th>Property</th>
										<th>Description</th>
										<th>Default value</th>
									</tr>
								</thead>
								<tbody>
									{props.map((prop) => (
										<tr key={prop.propertyName}>
											<td className="prop-name">{prop.propertyName}</td>
											<td className="prop-description">{prop.description}</td>
											<td className="prop-default">{prop.defaultValue}</td>
										</tr>
									))}
								</tbody>
							</table>
						)}
					</section>
				)
			})}
		</div>
	)
}
