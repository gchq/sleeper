import { Suspense, lazy } from 'react'
import { Navigate, RouterProvider, createBrowserRouter } from 'react-router-dom'
import Layout from './components/Layout'
import Data from './pages/Data'
import InstanceProperties from './pages/InstanceProperties'
import TableProperties from './pages/TableProperties'
import Tables from './pages/Tables'
import './App.css'

const DataMetricGraph = lazy(() => import('./pages/DataMetricGraph'))
const IngestBatcher = lazy(() => import('./pages/IngestBatcher'))
const IngestJobs = lazy(() => import('./pages/IngestJobs'))
const IngestJobDetail = lazy(() => import('./pages/IngestJobDetail'))
const Queries = lazy(() => import('./pages/Queries'))
const QueryDetail = lazy(() => import('./pages/QueryDetail'))
const SubQueryDetail = lazy(() => import('./pages/SubQueryDetail'))

const router = createBrowserRouter([
	{
		element: <Layout />,
		children: [
			{ path: '/', element: <Navigate to="/instance/properties" replace /> },
			{ path: '/instance/properties', element: <InstanceProperties /> },
			{ path: '/tables', element: <Tables /> },
			{
				path: '/ingest-batcher',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<IngestBatcher />
					</Suspense>
				),
			},
			{
				path: '/tables/:tableId/ingest-batcher',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<IngestBatcher />
					</Suspense>
				),
			},
			{
				path: '/ingest-jobs',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<IngestJobs />
					</Suspense>
				),
			},
			{
				path: '/tables/:tableId/ingest-jobs',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<IngestJobs />
					</Suspense>
				),
			},
			{
				path: '/ingest-jobs/:jobId',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<IngestJobDetail />
					</Suspense>
				),
			},
			{
				path: '/tables/:tableId/ingest-jobs/:jobId',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<IngestJobDetail />
					</Suspense>
				),
			},
			{
				path: '/queries',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<Queries />
					</Suspense>
				),
			},
			{
				path: '/queries/:queryId',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<QueryDetail />
					</Suspense>
				),
			},
			{
				path: '/queries/:queryId/:subQueryId',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<SubQueryDetail />
					</Suspense>
				),
			},
			{
				path: '/tables/:tableId/queries',
				element: (
					<Suspense fallback={<div className="page"><p>Loading...</p></div>}>
						<Queries />
					</Suspense>
				),
			},
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
