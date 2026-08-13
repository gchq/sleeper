import { Navigate, RouterProvider, createBrowserRouter } from 'react-router-dom'
import Layout from './components/Layout'
import InstanceProperties from './pages/InstanceProperties'
import TableProperties from './pages/TableProperties'
import './App.css'

const router = createBrowserRouter([
	{
		element: <Layout />,
		children: [
			{ path: '/', element: <Navigate to="/instance/properties" replace /> },
			{ path: '/instance/properties', element: <InstanceProperties /> },
			{ path: '/tables/:tableId/properties', element: <TableProperties /> },
			{ path: '*', element: <p className="placeholder">Select a section from the navigation.</p> },
		],
	},
])

function App() {
	return <RouterProvider router={router} />
}

export default App
