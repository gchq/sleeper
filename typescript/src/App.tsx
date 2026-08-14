import { Suspense, lazy } from 'react'
import { Navigate, RouterProvider, createBrowserRouter } from 'react-router-dom'
import Layout from './components/Layout'
import Data from './pages/Data'
import InstanceProperties from './pages/InstanceProperties'
import TableProperties from './pages/TableProperties'
import './App.css'

const DataMetricGraph = lazy(() => import('./pages/DataMetricGraph'))

const router = createBrowserRouter([
	{
		element: <Layout />,
		children: [
			{ path: '/', element: <Navigate to="/instance/properties" replace /> },
			{ path: '/instance/properties', element: <InstanceProperties /> },
			{ path: '/data', element: <Data /> },
			{
				path: '/data/graph/:group',
				element: (
					<Suspense fallback={<div className="page"><p>Loading graph...</p></div>}>
						<DataMetricGraph />
					</Suspense>
				),
			},
			{ path: '/tables/:tableId/properties', element: <TableProperties /> },
			{ path: '*', element: <p className="placeholder">Select a section from the navigation.</p> },
		],
	},
])

function App() {
	return <RouterProvider router={router} />
}

export default App
