import { BrowserRouter, NavLink, Route, Routes } from 'react-router-dom'
import InstanceProperties from './pages/InstanceProperties'
import './App.css'

function App() {
	return (
		<BrowserRouter>
			<div className="app">
				<header className="app-header">
					<h1>Sleeper</h1>
					<nav className="app-nav">
						<NavLink to="/instance/properties" className={({ isActive }) => (isActive ? 'active' : '')}>
							Instance Properties
						</NavLink>
					</nav>
				</header>
				<main className="app-main">
					<Routes>
						<Route path="/instance/properties" element={<InstanceProperties />} />
						<Route path="*" element={<p className="placeholder">Select a section from the navigation above.</p>} />
					</Routes>
				</main>
			</div>
		</BrowserRouter>
	)
}

export default App
