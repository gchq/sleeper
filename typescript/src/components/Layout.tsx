import { Outlet } from 'react-router-dom'
import Sidebar from './Sidebar'
import { InstanceProvider } from '../contexts/InstanceProvider'
import './Layout.css'

export default function Layout() {
	return (
		<InstanceProvider>
			<div className="app">
				<Sidebar />
				<main className="app-main">
					<Outlet />
				</main>
			</div>
		</InstanceProvider>
	)
}
