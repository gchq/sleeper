import PropertiesPage from '../components/PropertiesPage'
import Title from '../components/Title'
import { useSelectedTable } from '../hooks/useSelectedTable'

export default function TableProperties() {
	const { table, loading, error } = useSelectedTable()

	if (error) {
		return (
			<div className="page">
				<h2>Table Properties</h2>
				<p className="error">Failed to load tables: {error}</p>
			</div>
		)
	}

	if (loading && !table) {
		return (
			<div className="page">
				<h2>Table Properties</h2>
				<p>Loading...</p>
			</div>
		)
	}

	if (!table) {
		return (
			<div className="page">
				<h2>Table Properties</h2>
				<p className="error">Table not found.</p>
			</div>
		)
	}

	const encoded = encodeURIComponent(table.tableUniqueId)
	return (
		<>
			<Title>Table Properties: {table.tableName}</Title>

			<PropertiesPage
				adapter={{
					title: 'Table Properties',
					definitionsPath: '/sleeper/table/properties',
					valuesPath: `/tables/${encoded}/properties`,
					validatePath: '/sleeper/table/properties/validate',
					savePath: `/tables/${encoded}/properties`,
				}}
			/>
		</>
	)
}
