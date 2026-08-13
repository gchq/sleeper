import { Outlet } from 'react-router-dom'
import Sidebar from './Sidebar'
import { TablesProvider } from '../contexts/TablesContext'

export default function Layout() {
	return (
		<TablesProvider>
			<div className="app">
				<Sidebar />
				<main className="app-main">
					<Outlet />
				</main>
			</div>
		</TablesProvider>
	)
}
