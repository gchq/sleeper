import { useEffect, useState } from 'react'
import './App.css'

interface HealthStatus {
	status: string
}

function App() {
	const [health, setHealth] = useState<HealthStatus | null>(null)
	const [error, setError] = useState<string | null>(null)

	useEffect(() => {
		fetch('/api/health')
			.then((response) => {
				if (!response.ok) {
					throw new Error(`HTTP error: ${response.status}`)
				}
				return response.json()
			})
			.then((data: HealthStatus) => setHealth(data))
			.catch((err: Error) => setError(err.message))
	}, [])

	return (
		<div className="app">
			<h1>Sleeper</h1>
			<div className="status-card">
				<h2>API Health Check</h2>
				{error && <p className="error">Error: {error}</p>}
				{health && <p className="success">Status: {health.status}</p>}
				{!health && !error && <p>Checking API health...</p>}
			</div>
		</div>
	)
}

export default App