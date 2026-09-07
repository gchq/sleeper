import { Suspense, lazy, useState } from 'react'
import { Link } from 'react-router-dom'
import { AmazonDynamoDb, AmazonSimpleQueueService, AwsLambda } from '@aws-icons/react/architecture-service'
import Title from '../components/Title'
import HelpIcon from '../components/icons/HelpIcon'
import type { InstanceFeatures, TableStatus } from '../contexts/InstanceContext'
import { FEATURES, FEATURE_GROUPS, type FeatureIcon, type FeatureInfo, type FeatureName } from '../lib/features'
import { useSelectedTable } from '../hooks/useSelectedTable'
import './Home.css'

const CreateTableWizard = lazy(() => import('./CreateTableWizard'))
const IngestFileWizard = lazy(() => import('../components/IngestFileWizard'))
const QueryWizard = lazy(() => import('../components/QueryWizard'))
const EnableFeatureModal = lazy(() => import('../components/EnableFeatureModal'))

type WizardName = 'createTable' | 'ingest' | 'query'

interface QuickStartStep {
	key: WizardName
	label: string
	description: string
	icon: FeatureIcon
	feature?: FeatureName
	needsTable: boolean
}

const QUICK_START: QuickStartStep[] = [
	{
		key: 'createTable',
		label: 'Create a table',
		description: 'Define a schema and add a table to this instance.',
		icon: AmazonDynamoDb,
		needsTable: false,
	},
	{
		key: 'ingest',
		label: 'Ingest some data',
		description: 'Submit Parquet files in S3 to be written into a table.',
		icon: AmazonSimpleQueueService,
		feature: 'IngestBatcherStack',
		needsTable: true,
	},
	{
		key: 'query',
		label: 'Run a query',
		description: 'Look up rows by their row key and see the results.',
		icon: AwsLambda,
		feature: 'QueryStack',
		needsTable: true,
	},
]

function disabledReason(
	step: QuickStartStep,
	features: InstanceFeatures | null,
	tables: TableStatus[] | null,
	loading: boolean,
): string | null {
	if (!features) return loading ? 'Loading…' : 'Instance unavailable'
	if (step.feature && !features[step.feature]) return 'Not enabled for this instance'
	if (step.needsTable && (!tables || tables.length === 0)) return 'Create a table first'
	return null
}

function FeatureCard({
	feature,
	enabled,
	onHelp,
}: {
	feature: FeatureInfo
	enabled: boolean
	onHelp: (feature: FeatureInfo) => void
}) {
	const Icon = feature.icon
	const path = enabled ? feature.path : undefined

	const body = (
		<>
			<span className="home-feature-icon" aria-hidden="true">
				<Icon width={36} height={36} />
			</span>
			<span className="home-feature-text">
				<span className="home-feature-name">
					<span>{feature.name}</span>
					<span className={enabled ? 'home-feature-badge enabled' : 'home-feature-badge'}>
						{enabled ? 'Enabled' : 'Not deployed'}
					</span>
				</span>
				<span className="home-feature-description">{feature.description}</span>
			</span>
			{path && (
				<span className="home-feature-chevron" aria-hidden="true">
					›
				</span>
			)}
			{!enabled && (
				<span className="home-feature-help" aria-hidden="true">
					<HelpIcon />
				</span>
			)}
		</>
	)

	return (
		<li className={enabled ? 'home-feature' : 'home-feature home-feature-disabled'}>
			{path ? (
				<Link to={path} className="home-feature-inner" title={'Go to ' + feature.name}>
					{body}
				</Link>
			) : !enabled ? (
				<button
					type="button"
					className="home-feature-inner"
					onClick={() => onHelp(feature)}
					title={'How to enable ' + feature.name}
					aria-label={'How to enable ' + feature.name}
				>
					{body}
				</button>
			) : (
				<div className="home-feature-inner">{body}</div>
			)}
		</li>
	)
}

export default function Home() {
	const { instanceId, region, tables, features, table, loading, error, reload } = useSelectedTable()
	const [wizard, setWizard] = useState<WizardName | null>(null)
	const [helpFeature, setHelpFeature] = useState<FeatureInfo | null>(null)

	return (
		<>
			<Title></Title>

			<div className="page">
				<h2 className="home-heading">
					{instanceId}
					{region && (
						<span className="home-region" title={'AWS region: ' + region}>
							{region}
						</span>
					)}
				</h2>

				{error && <p className="error">Failed to load instance: {error}</p>}

				<section className="home-section">
					<h2>Quick start</h2>
					<p className="home-section-intro">Follow these steps to start exploring your instance.</p>
					<ol className="home-steps">
						{QUICK_START.map((step, i) => {
							const reason = disabledReason(step, features, tables, loading)
							const Icon = step.icon
							return (
								<li key={step.key} className="home-step-item">
									<button
										type="button"
										className="home-step"
										onClick={() => setWizard(step.key)}
										disabled={reason !== null}
										title={reason ?? step.description}
									>
										<span className="home-step-icon" aria-hidden="true">
											<Icon width={40} height={40} />
										</span>
										<span className="home-step-body">
											<span className="home-step-label">
												<span className="home-step-index" aria-hidden="true">
													{i + 1}
												</span>
												{step.label}
											</span>
											<span className="home-step-description">{step.description}</span>
											{reason && <span className="home-step-hint">{reason}</span>}
										</span>
									</button>
								</li>
							)
						})}
					</ol>
				</section>

				<section className="home-section">
					<h3>Features</h3>
					<p className="home-section-intro">
						This is a list of all the features that a Sleeper instance can be deployed with.
					</p>
					{!features ? (
						<p className="placeholder">{loading ? 'Loading features...' : 'Features unavailable.'}</p>
					) : (
						FEATURE_GROUPS.map(group => (
							<div key={group} className="home-feature-group">
								<div className="home-feature-group-label">{group}</div>
								<ul className="home-feature-grid">
									{FEATURES.filter(f => f.group === group).map((f) => (
										<FeatureCard key={f.key} feature={f} enabled={!!features[f.key]} onHelp={setHelpFeature} />
									))}
								</ul>
							</div>
						))
					)}
				</section>
			</div>

			<Suspense fallback={null}>
				{wizard === 'createTable' && (
					<CreateTableWizard onClose={() => setWizard(null)} onCreated={() => reload()} />
				)}
				{wizard === 'ingest' && (
					<IngestFileWizard
						onClose={() => setWizard(null)}
						defaultMethod="ingest_batcher"
						presetTableId={table?.tableUniqueId}
					/>
				)}
				{wizard === 'query' && <QueryWizard onClose={() => setWizard(null)} presetTableId={table?.tableUniqueId} />}
				{helpFeature && <EnableFeatureModal feature={helpFeature} onClose={() => setHelpFeature(null)} />}
			</Suspense>
		</>
	)
}
