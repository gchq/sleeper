import { useEffect, useMemo, useState } from 'react'
import { postJson } from '../lib/api'
import { AmazonSimpleStorageService } from '@aws-icons/react/architecture-service'
import { s3ConsoleHomeUrl, s3ConsoleUrl } from '../lib/aws'
import { useInstance } from '../contexts/InstanceContext'
import { formatBytes } from '../lib/dataMetrics'
import './IngestFileWizard.css'

export type IngestMethod = 'ingest_batcher'

const METHOD_LABELS: Record<IngestMethod, string> = {
	ingest_batcher: 'Ingest Batcher',
}

interface Props {
	onClose: () => void
	onSubmitted?: () => void
	defaultMethod?: IngestMethod
	presetTableId?: string
}

interface ExpandedFile {
	file: string
	fileSizeBytes: number
}

interface ExpandedPath {
	requestedPath: string
	prefix: boolean
	empty: boolean
	tooMany: boolean
	files: ExpandedFile[]
}

interface ExpandResponse {
	paths: ExpandedPath[]
	missingPaths: string[]
	maxFilesPerPath: number
}

interface CompatibleTable {
	tableId: string
	tableName: string
}

interface InspectResponse {
	fileSchema: string | null
	tables: CompatibleTable[]
}

interface SubmittedTable {
	tableName: string
	fileCount: number
}

interface SubmitResponse {
	submitted: SubmittedTable[]
	method: string
}

const STEPS = ['Paths', 'Files', 'Tables', 'Method', 'Review'] as const

async function readError(resp: Response): Promise<string> {
	try {
		const body = await resp.json()
		if (body && typeof body === 'object') {
			return body.message || body.reason || JSON.stringify(body)
		}
	} catch {
		// non-JSON body
	}
	return `HTTP ${resp.status}`
}

export default function IngestFileWizard({ onClose, onSubmitted, defaultMethod = 'ingest_batcher', presetTableId }: Props) {
	const { region, features } = useInstance()

	const [step, setStep] = useState(0)
	const [pathsRaw, setPathsRaw] = useState('')

	// Expanded files, keyed by the input path, with an include flag.
	const [expanded, setExpanded] = useState<ExpandResponse | null>(null)
	const [included, setIncluded] = useState<Record<string, boolean>>({})

	const [inspect, setInspect] = useState<InspectResponse | null>(null)
	const [selectedTableIds, setSelectedTableIds] = useState<string[]>([])
	const [method, setMethod] = useState<IngestMethod>(defaultMethod)

	const [stepError, setStepError] = useState<string | null>(null)
	const [checking, setChecking] = useState(false)
	const [submitting, setSubmitting] = useState(false)
	const [submitError, setSubmitError] = useState<string | null>(null)
	const [success, setSuccess] = useState<SubmitResponse | null>(null)

	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onClose()
		}
		window.addEventListener('keydown', onKey)
		return () => window.removeEventListener('keydown', onKey)
	}, [onClose])

	const allFiles = useMemo(() => expanded?.paths.flatMap((p) => p.files) ?? [], [expanded])
	const selectedFiles = useMemo(() => allFiles.filter((f) => included[f.file]).map((f) => f.file), [allFiles, included])

	function goBack() {
		setStepError(null)
		setStep((s) => Math.max(0, s - 1))
	}

	async function goNext() {
		setStepError(null)

		if (step === 0) {
			const paths = pathsRaw.split('\n').map((l) => l.trim()).filter((l) => l !== '')
			if (paths.length === 0) {
				setStepError('Enter at least one S3 path.')
				return
			}
			setChecking(true)
			try {
				const resp = await postJson('/ingest-file/expand', { paths })
				if (!resp.ok) {
					setStepError(await readError(resp))
					return
				}
				const result: ExpandResponse = await resp.json()
				setExpanded(result)
				// Default every discovered file to included.
				const includeMap: Record<string, boolean> = {}
				result.paths.forEach((p) => p.files.forEach((f) => (includeMap[f.file] = true)))
				setIncluded(includeMap)
				setStep(1)
			} catch (err) {
				setStepError((err as Error).message)
			} finally {
				setChecking(false)
			}
			return
		}

		if (step === 1) {
			const overflowing = expanded?.paths.filter((p) => p.tooMany) ?? []
			if (overflowing.length > 0) {
				const limit = expanded?.maxFilesPerPath
				setStepError(
					`Too many files under ${overflowing.map((p) => p.requestedPath).join(', ')}` +
						(limit ? ` (more than ${limit})` : '') +
						'. Narrow the prefix to a more specific path.',
				)
				return
			}
			if (selectedFiles.length === 0) {
				setStepError('Select at least one file to ingest.')
				return
			}
			setChecking(true)
			try {
				const resp = await postJson('/ingest-file/inspect', { files: selectedFiles })
				if (!resp.ok) {
					setStepError(await readError(resp))
					return
				}
				const result: InspectResponse = await resp.json()
				setInspect(result)
				// Preselect the host table if it is among the compatible tables.
				if (presetTableId && result.tables.some((t) => t.tableId === presetTableId)) {
					setSelectedTableIds([presetTableId])
				}
				setStep(2)
			} catch (err) {
				setStepError((err as Error).message)
			} finally {
				setChecking(false)
			}
			return
		}

		if (step === 2) {
			if (selectedTableIds.length === 0) {
				setStepError('Select at least one table.')
				return
			}
			setStep(3)
			return
		}

		if (step === 3) {
			if (method === 'ingest_batcher' && !features?.IngestBatcherStack) {
				setStepError('The ingest batcher is not enabled for this instance.')
				return
			}
			setStep(4)
			return
		}
	}

	async function submit() {
		setSubmitError(null)
		setSubmitting(true)
		try {
			const resp = await postJson('/ingest-file/submit', {
				files: selectedFiles,
				tableIds: selectedTableIds,
				method,
			})
			if (resp.status === 201) {
				const body: SubmitResponse = await resp.json()
				setSuccess(body)
				onSubmitted?.()
				return
			}
			setSubmitError(await readError(resp))
		} catch (err) {
			setSubmitError((err as Error).message)
		} finally {
			setSubmitting(false)
		}
	}

	function toggleFile(file: string) {
		setIncluded((prev) => ({ ...prev, [file]: !prev[file] }))
	}

	function toggleTable(tableId: string) {
		setSelectedTableIds((prev) =>
			prev.includes(tableId) ? prev.filter((id) => id !== tableId) : [...prev, tableId],
		)
	}

	if (success) {
		const totalFiles = success.submitted.reduce((sum, s) => sum + s.fileCount, 0)
		return (
			<div className="modal-backdrop" onClick={onClose}>
				<div className="modal ingest-file-modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
					<h3 className="modal-title">Files submitted</h3>
					<div className="ingest-file-success">
						<p>
							Submitted <strong>{totalFiles}</strong> file{totalFiles === 1 ? '' : 's'} to{' '}
							<strong>{success.submitted.length}</strong> table{success.submitted.length === 1 ? '' : 's'} via{' '}
							<strong>{METHOD_LABELS[success.method as IngestMethod] ?? success.method}</strong>.
						</p>
						<ul className="ingest-file-success-list">
							{success.submitted.map((s) => (
								<li key={s.tableName}>
									<strong>{s.tableName}</strong> — {s.fileCount} file{s.fileCount === 1 ? '' : 's'}
								</li>
							))}
						</ul>
					</div>
					<div className="modal-actions">
						<span style={{ flex: 1 }} />
						<button className="btn btn-primary" onClick={onClose}>
							Done
						</button>
					</div>
				</div>
			</div>
		)
	}

	const isLastStep = step === STEPS.length - 1

	return (
		<div className="modal-backdrop" onClick={onClose}>
			<form
				className="modal ingest-file-modal"
				onClick={(e) => e.stopPropagation()}
				role="dialog"
				aria-modal="true"
				onSubmit={(e) => {
					e.preventDefault()
					if (isLastStep) {
						if (!submitting) submit()
					} else if (!checking) {
						goNext()
					}
				}}
			>
				<h3 className="modal-title">Ingest File</h3>

				<ol className="ingest-file-steps">
					{STEPS.map((label, i) => (
						<li
							key={label}
							className={i === step ? 'ingest-file-step active' : i < step ? 'ingest-file-step done' : 'ingest-file-step'}
						>
							<span className="ingest-file-step-num">{i + 1}</span>
							<span className="ingest-file-step-label">{label}</span>
						</li>
					))}
				</ol>

				<div className="ingest-file-body">
					{step === 0 && <PathsStep value={pathsRaw} onChange={setPathsRaw} region={region} />}
					{step === 1 && expanded && (
						<FilesStep expanded={expanded} included={included} onToggle={toggleFile} region={region} />
					)}
					{step === 2 && inspect && (
						<TablesStep
							tables={inspect.tables}
							fileSchema={inspect.fileSchema}
							selected={selectedTableIds}
							onToggle={toggleTable}
						/>
					)}
					{step === 3 && (
						<MethodStep method={method} onChange={setMethod} batcherEnabled={!!features?.IngestBatcherStack} />
					)}
					{step === 4 && (
						<ReviewStep
							files={selectedFiles}
							tables={(inspect?.tables ?? []).filter((t) => selectedTableIds.includes(t.tableId))}
							method={method}
						/>
					)}
				</div>

				<div className="ingest-file-footer">
					{stepError && <p className="error modal-error">{stepError}</p>}
					{isLastStep && submitError && <p className="error modal-error">Submit failed: {submitError}</p>}

					<div className="modal-actions">
						{!isLastStep && (
							<button type="submit" className="btn btn-primary" style={{ order: 4 }} disabled={checking}>
								{checking ? 'Checking…' : 'Next'}
							</button>
						)}
						{isLastStep && (
							<button type="submit" className="btn btn-primary" style={{ order: 4 }} disabled={submitting}>
								{submitting ? 'Submitting…' : 'Submit'}
							</button>
						)}
						{step > 0 && (
							<button type="button" className="btn" style={{ order: 3 }} onClick={goBack} disabled={checking || submitting}>
								Back
							</button>
						)}
						<button type="button" className="btn" style={{ order: 1 }} onClick={onClose} disabled={submitting}>
							Cancel
						</button>
						<span style={{ flex: 1, order: 2 }} />
					</div>
				</div>
			</form>
		</div>
	)
}

function PathsStep({ value, onChange, region }: { value: string; onChange: (v: string) => void, region: string | null}) {
	return (
		<div className="ingest-file-field-block">
			<p className="modal-description">
				Enter one or more S3 paths, one per line. Each can be a single Parquet object or a prefix/directory
				(all Parquet files underneath will be discovered).
			</p>
			<label className="modal-field">
				<div className="ingest-file-paths-label">
					<span className="modal-field-label">S3 paths</span>
					{region && (
						<a
							className="btn ingest-file-explore"
							href={s3ConsoleHomeUrl(region)}
							target="_blank"
							rel="noopener noreferrer"
						>
							<AmazonSimpleStorageService width={14} height={14} />
							Explore S3
						</a>
					)}
				</div>
				<textarea
					className="modal-input ingest-file-paths"
					value={value}
					onChange={(e) => onChange(e.target.value)}
					rows={7}
					placeholder={'s3://bucket-name/prefix/\nbucket-name/path/to/file.parquet'}
					autoFocus
				/>
			</label>
		</div>
	)
}

function FilesStep({
	region,
	expanded,
	included,
	onToggle,
}: {
	region: string | null
	expanded: ExpandResponse
	included: Record<string, boolean>
	onToggle: (file: string) => void
}) {
	return (
		<div className="ingest-file-field-block">
			{expanded.missingPaths.length > 0 && (
				<p className="error modal-error">
					No files found for: {expanded.missingPaths.join(', ')}
				</p>
			)}
			<p className="modal-description">
				Confirm which files to ingest. Prefixes have been expanded to the Parquet files found underneath.
			</p>
			{expanded.paths.map((p) => {
				const consoleUrl = p.tooMany && region ? s3ConsoleUrl(p.requestedPath, region) : null
				return (
				<div key={p.requestedPath} className="ingest-file-path-group">
					<div className="ingest-file-path-head">
						<code>{p.requestedPath}</code>
						{p.prefix && <span className="ingest-file-badge">prefix</span>}
					</div>
					{p.tooMany ? (
						<p className="error modal-error">
							This prefix contains more than {expanded.maxFilesPerPath} files. Narrow it to a more specific path.
							{consoleUrl && (
								<>
									{' '}
									<a
										className="ingest-file-console-link"
										href={consoleUrl}
										target="_blank"
										rel="noopener noreferrer"
									>
										<AmazonSimpleStorageService width={13} height={13} />
										Browse in S3 console
									</a>
								</>
							)}
						</p>
					) : p.files.length === 0 ? (
						<p className="ingest-file-empty">No Parquet files found.</p>
					) : (
						<ul className="ingest-file-list">
							{p.files.map((f) => (
								<li key={f.file}>
									<label className="ingest-file-check">
										<input type="checkbox" checked={!!included[f.file]} onChange={() => onToggle(f.file)} />
										<code className="ingest-file-name">{f.file}</code>
										<span className="ingest-file-size">{formatBytes(f.fileSizeBytes)}</span>
									</label>
								</li>
							))}
						</ul>
					)}
				</div>
			)
			})}
		</div>
	)
}

function TablesStep({
	tables,
	fileSchema,
	selected,
	onToggle,
}: {
	tables: CompatibleTable[]
	fileSchema: string | null
	selected: string[]
	onToggle: (tableId: string) => void
}) {
	return (
		<div className="ingest-file-field-block">
			<p className="modal-description">
				Choose one or more tables to ingest into. Only tables whose schema matches the selected files are shown.
			</p>
			{tables.length === 0 ? (
				<div className="ingest-file-no-tables">
					<p className="ingest-file-empty">No tables have a compatible schema for these files.</p>
					{fileSchema && (
						<div className="ingest-file-schema">
							<span className="modal-field-label">Schema of the selected files</span>
							<pre className="ingest-file-schema-json">{fileSchema}</pre>
						</div>
					)}
				</div>
			) : (
				<ul className="ingest-file-list">
					{tables.map((t) => (
						<li key={t.tableId}>
							<label className="ingest-file-check">
								<input type="checkbox" checked={selected.includes(t.tableId)} onChange={() => onToggle(t.tableId)} />
								<span className="ingest-file-table-name">{t.tableName}</span>
							</label>
						</li>
					))}
				</ul>
			)}
		</div>
	)
}

function MethodStep({
	method,
	onChange,
	batcherEnabled,
}: {
	method: IngestMethod
	onChange: (m: IngestMethod) => void
	batcherEnabled: boolean
}) {
	return (
		<div className="ingest-file-field-block">
			<p className="modal-description">Choose how the files should be ingested into Sleeper.</p>
			{!batcherEnabled && (
				<div className="ingest-file-unavailable">
					<p>The Ingest Batcher component isn't available for this instance.</p>
					<p>
						To enable it, add <code>IngestBatcherStack</code> to the <code>sleeper.optional.stacks</code> instance
						property, then redeploy the instance.
					</p>
				</div>
			)}
			<label className="ingest-file-method">
				<input
					type="radio"
					name="ingest-method"
					value="ingest_batcher"
					checked={method === 'ingest_batcher'}
					onChange={() => onChange('ingest_batcher')}
					disabled={!batcherEnabled}
				/>
				<span>
					<strong>{METHOD_LABELS.ingest_batcher}</strong>
					<span className="ingest-file-method-hint">Files are batched into ingest jobs by the ingest batcher.</span>
				</span>
			</label>
		</div>
	)
}

function ReviewStep({
	files,
	tables,
	method,
}: {
	files: string[]
	tables: CompatibleTable[]
	method: IngestMethod
}) {
	return (
		<div className="ingest-file-review">
			<div className="ingest-file-review-row">
				<span className="modal-field-label">Files</span>
				<span className="ingest-file-review-value">{files.length} file{files.length === 1 ? '' : 's'}</span>
			</div>
			<div className="ingest-file-review-row">
				<span className="modal-field-label">Tables</span>
				<span className="ingest-file-review-value">{tables.map((t) => t.tableName).join(', ')}</span>
			</div>
			<div className="ingest-file-review-row">
				<span className="modal-field-label">Method</span>
				<span className="ingest-file-review-value">{METHOD_LABELS[method]}</span>
			</div>
		</div>
	)
}