import { useState } from 'react'
import { Link } from 'react-router-dom'
import Title from '../components/Title'
import { useTablesList } from '../contexts/TablesContext'
import CreateTableWizard from './CreateTableWizard'
import './Tables.css'

export default function Tables() {
	const { tables, loading, error, reload } = useTablesList()
	const [wizardOpen, setWizardOpen] = useState(false)

	return (
		<>
			<Title>Tables</Title>
			<div className="page">
				<div className="tables-header">
					<h2>Tables</h2>
					<button className="btn btn-primary" onClick={() => setWizardOpen(true)}>
						Create table
					</button>
				</div>

				{error && <p className="error">Failed to load tables: {error}</p>}

				{!error && !tables && <p>{loading ? 'Loading...' : 'No data.'}</p>}

				{!error && tables && tables.length === 0 && (
					<p className="tables-empty">
						No tables yet. Click <strong>Create table</strong> to add one.
					</p>
				)}

				{!error && tables && tables.length > 0 && (
					<table className="tables-table">
						<thead>
							<tr>
								<th>Name</th>
								<th>Table ID</th>
								<th>Status</th>
								<th />
							</tr>
						</thead>
						<tbody>
							{tables.map((t) => (
								<tr key={t.tableUniqueId}>
									<td className="tables-name">{t.tableName}</td>
									<td className="tables-id">
										<code>{t.tableUniqueId}</code>
									</td>
									<td>
										<span className={t.online ? 'tables-status online' : 'tables-status offline'}>
											{t.online ? 'Online' : 'Offline'}
										</span>
									</td>
									<td className="tables-actions">
										<Link
											to={`/tables/${encodeURIComponent(t.tableUniqueId)}/properties`}
											className="btn-link"
										>
											Properties
										</Link>
									</td>
								</tr>
							))}
						</tbody>
					</table>
				)}
			</div>

			{wizardOpen && (
				<CreateTableWizard
					onClose={() => setWizardOpen(false)}
					onCreated={() => reload()}
				/>
			)}
		</>
	)
}
