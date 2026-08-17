import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { apiFetch, postJson, postRaw } from '../lib/api'
import { useTablesList, type TableStatus } from '../contexts/TablesContext'
import {
	PRIMITIVE_TYPES,
	PRIMITIVE_TYPE_LABELS,
	SCHEMA_TEMPLATES,
	firstRowKeyType,
	newKeyField,
	newValueField,
	parseSchemaString,
	schemaDraftToJsonString,
	splitPointExampleLines,
	splitPointExamplesForType,
	type KeyFieldDraft,
	type PrimitiveTypeName,
	type SchemaDraft,
	type ValueFieldDraft,
	type ValueTypeKind,
} from '../lib/tableSchema'
import './CreateTableWizard.css'

const TABLE_NAME_PROPERTY = 'sleeper.table.name'
const TABLE_SCHEMA_PROPERTY = 'sleeper.table.schema'

interface ValidationResult {
	valid: boolean
	reason: string | null
}

interface CreateSuccess {
	tableId: string
	tableName: string
}

interface Props {
	onClose: () => void
	onCreated: () => void
}

const STEPS = ['Name', 'Schema', 'Split points', 'Review'] as const

export default function CreateTableWizard({ onClose, onCreated }: Props) {
	const { tables } = useTablesList()
	const [step, setStep] = useState(0)
	const [name, setName] = useState('')
	const [schema, setSchema] = useState<SchemaDraft>({
		rowKeys: [newKeyField()],
		sortKeys: [],
		valueFields: [newValueField()],
	})
	const [splitPointsRaw, setSplitPointsRaw] = useState('')
	const [stepError, setStepError] = useState<string | null>(null)
	const [checking, setChecking] = useState(false)
	const [submitting, setSubmitting] = useState(false)
	const [submitError, setSubmitError] = useState<string | null>(null)
	const [success, setSuccess] = useState<CreateSuccess | null>(null)

	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onClose()
		}
		window.addEventListener('keydown', onKey)
		return () => window.removeEventListener('keydown', onKey)
	}, [onClose])

	const rowKeyType = firstRowKeyType(schema)

	function goBack() {
		setStepError(null)
		setStep((s) => Math.max(0, s - 1))
	}

	async function goNext() {
		setStepError(null)
		if (step === 0) {
			if (!name.trim()) {
				setStepError('Table name is required.')
				return
			}
			setStep(1)
			return
		}
		if (step === 1) {
			// Validate schema server-side.
			setChecking(true)
			try {
				const resp = await postRaw('/sleeper/schema/validate', schemaDraftToJsonString(schema))
				if (!resp.ok) {
					setStepError(`Validation request failed: HTTP ${resp.status}`)
					return
				}
				const result: ValidationResult = await resp.json()
				if (!result.valid) {
					setStepError(result.reason || 'Schema is not valid.')
					return
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
			const lines = splitPointsRaw.split('\n').map((l) => l.trim()).filter((l) => l !== '')
			if (lines.length === 0) {
				setStep(3)
				return
			}
			setChecking(true)
			try {
				const resp = await postJson('/sleeper/split-points/validate', {
					schema: schemaDraftToJsonString(schema),
					splitPoints: lines,
				})
				if (!resp.ok) {
					setStepError(`Validation request failed: HTTP ${resp.status}`)
					return
				}
				const result: ValidationResult = await resp.json()
				if (!result.valid) {
					setStepError(result.reason || 'Split points are not valid.')
					return
				}
				setStep(3)
			} catch (err) {
				setStepError((err as Error).message)
			} finally {
				setChecking(false)
			}
			return
		}
	}

	async function submit() {
		setSubmitError(null)
		setSubmitting(true)
		try {
			const lines = splitPointsRaw.split('\n').map((l) => l.trim()).filter((l) => l !== '')
			const resp = await postJson('/tables', {
				properties: { [TABLE_NAME_PROPERTY]: name.trim() },
				schema: schemaDraftToJsonString(schema),
				splitPoints: lines,
			})
			if (resp.status === 201) {
				const body: CreateSuccess = await resp.json()
				setSuccess(body)
				onCreated()
				return
			}
			if (resp.status === 409) {
				setSubmitError(null)
				setStepError('A table with this name already exists.')
				setStep(0)
				return
			}
			// 400 or other — surface the message.
			let message = `HTTP ${resp.status}`
			try {
				const body = await resp.json()
				if (body && typeof body === 'object') {
					message = body.message || body.reason || JSON.stringify(body)
				}
			} catch {
				// non-JSON body
			}
			setSubmitError(message)
		} catch (err) {
			setSubmitError((err as Error).message)
		} finally {
			setSubmitting(false)
		}
	}

	function loadTemplate(id: string) {
		const template = SCHEMA_TEMPLATES.find((t) => t.id === id)
		if (!template) return
		// Deep-copy so edits don't mutate the template.
		setSchema({
			rowKeys: template.schema.rowKeys.map((f) => ({ ...f })),
			sortKeys: template.schema.sortKeys.map((f) => ({ ...f })),
			valueFields: template.schema.valueFields.map((f) => ({ ...f })),
		})
		setStepError(null)
	}

	async function loadFromTable(tableId: string) {
		setStepError(null)
		setChecking(true)
		try {
			const resp = await apiFetch('/tables/' + encodeURIComponent(tableId) + '/properties')
			if (!resp.ok) {
				setStepError(`Could not load table schema: HTTP ${resp.status}`)
				return
			}
			const props: Record<string, string> = await resp.json()
			const schemaJson = props[TABLE_SCHEMA_PROPERTY]
			if (!schemaJson) {
				setStepError('That table has no schema to copy.')
				return
			}
			setSchema(parseSchemaString(schemaJson))
		} catch (err) {
			setStepError((err as Error).message)
		} finally {
			setChecking(false)
		}
	}

	function loadSplitPointExample(id: string) {
		const example = splitPointExamplesForType(rowKeyType).find((e) => e.id === id)
		if (!example) return
		setSplitPointsRaw(splitPointExampleLines(example).join('\n'))
		setStepError(null)
	}

	async function loadSplitPointsFromTable(tableId: string) {
		setStepError(null)
		setChecking(true)
		try {
			const resp = await apiFetch('/tables/' + encodeURIComponent(tableId) + '/split-points')
			if (!resp.ok) {
				setStepError(`Could not load split points: HTTP ${resp.status}`)
				return
			}
			const body: { splitPoints: string[] } = await resp.json()
			setSplitPointsRaw((body.splitPoints ?? []).join('\n'))
		} catch (err) {
			setStepError((err as Error).message)
		} finally {
			setChecking(false)
		}
	}

	// ── Success panel ──
	if (success) {
		return (
			<div className="modal-backdrop" onClick={onClose}>
				<div className="modal wizard-modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
					<h3 className="modal-title">Table created</h3>
					<div className="wizard-success">
						<p>
							Table <strong>{success.tableName}</strong> was created successfully.
						</p>
						<div className="wizard-success-id">
							<span className="modal-field-label">Generated table ID</span>
							<code>{success.tableId}</code>
						</div>
					</div>
					<div className="modal-actions">
						<span style={{ flex: 1 }} />
						<button className="btn" onClick={onClose}>
							Back to tables
						</button>
						<Link
							className="btn btn-primary"
							to={`/tables/${encodeURIComponent(success.tableId)}/properties`}
							onClick={onClose}
						>
							View / edit properties
						</Link>
					</div>
				</div>
			</div>
		)
	}

	const isLastStep = step === STEPS.length - 1

	return (
		<div className="modal-backdrop" onClick={onClose}>
			<form
				className="modal wizard-modal"
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
				<h3 className="modal-title">Create a table</h3>

				<ol className="wizard-steps">
					{STEPS.map((label, i) => (
						<li
							key={label}
							className={
								i === step ? 'wizard-step active' : i < step ? 'wizard-step done' : 'wizard-step'
							}
						>
							<span className="wizard-step-num">{i + 1}</span>
							<span className="wizard-step-label">{label}</span>
						</li>
					))}
				</ol>

				<div className="wizard-body">
					{step === 0 && (
						<NameStep name={name} onChange={setName} />
					)}
					{step === 1 && (
						<SchemaStep
							schema={schema}
							onChange={setSchema}
							onLoadTemplate={loadTemplate}
							tables={tables}
							onLoadFromTable={loadFromTable}
						/>
					)}
					{step === 2 && (
						<SplitPointsStep
							rowKeyType={rowKeyType}
							value={splitPointsRaw}
							onChange={setSplitPointsRaw}
							onLoadExample={loadSplitPointExample}
							tables={tables}
							onLoadFromTable={loadSplitPointsFromTable}
						/>
					)}
					{step === 3 && (
						<ReviewStep name={name} schema={schema} splitPointsRaw={splitPointsRaw} />
					)}
				</div>

				<div className="wizard-footer">
					{stepError && <p className="error modal-error">{stepError}</p>}
					{isLastStep && submitError && (
						<p className="error modal-error">Create failed: {submitError}</p>
					)}

					<div className="modal-actions">
						<button type="button" className="btn" onClick={onClose} disabled={submitting}>
							Cancel
						</button>
						<span style={{ flex: 1 }} />
						{step > 0 && (
							<button type="button" className="btn" onClick={goBack} disabled={checking || submitting}>
								Back
							</button>
						)}
						{!isLastStep && (
							<button type="submit" className="btn btn-primary" disabled={checking}>
								{checking ? 'Checking…' : 'Next'}
							</button>
						)}
						{isLastStep && (
							<button type="submit" className="btn btn-primary" disabled={submitting}>
								{submitting ? 'Creating…' : 'Create table'}
							</button>
						)}
					</div>
				</div>
			</form>
		</div>
	)
}

// ── Step 1: Name ──

function NameStep({ name, onChange }: { name: string; onChange: (v: string) => void }) {
	return (
		<div className="wizard-field-block">
			<label className="modal-field">
				<span className="modal-field-label">Table name</span>
				<input
					className="modal-input"
					type="text"
					value={name}
					onChange={(e) => onChange(e.target.value)}
					placeholder="e.g. my-table"
					autoFocus
				/>
			</label>
			<p className="modal-note">
				The table name is permanent and must be unique within the instance.
			</p>
		</div>
	)
}

// ── Step 2: Schema ──

function SchemaStep({
	schema,
	onChange,
	onLoadTemplate,
	tables,
	onLoadFromTable,
}: {
	schema: SchemaDraft
	onChange: (s: SchemaDraft) => void
	onLoadTemplate: (id: string) => void
	tables: TableStatus[] | null
	onLoadFromTable: (tableId: string) => void
}) {
	function updateRowKey(i: number, field: KeyFieldDraft) {
		const rowKeys = schema.rowKeys.slice()
		rowKeys[i] = field
		onChange({ ...schema, rowKeys })
	}
	function updateSortKey(i: number, field: KeyFieldDraft) {
		const sortKeys = schema.sortKeys.slice()
		sortKeys[i] = field
		onChange({ ...schema, sortKeys })
	}
	function updateValueField(i: number, field: ValueFieldDraft) {
		const valueFields = schema.valueFields.slice()
		valueFields[i] = field
		onChange({ ...schema, valueFields })
	}

	return (
		<div className="wizard-schema">
			<div className="wizard-picker-row">
				<div className="wizard-template-picker">
					<span className="modal-field-label">Start from a template</span>
					<select
						defaultValue=""
						onChange={(e) => {
							if (e.target.value) onLoadTemplate(e.target.value)
							e.target.value = ''
						}}
					>
						<option value="" disabled>
							Choose a template…
						</option>
						{SCHEMA_TEMPLATES.map((t) => (
							<option key={t.id} value={t.id} title={t.description}>
								{t.label}
							</option>
						))}
					</select>
				</div>

				<div className="wizard-template-picker">
					<span className="modal-field-label">Copy from an existing table</span>
					<select
						defaultValue=""
						disabled={!tables || tables.length === 0}
						onChange={(e) => {
							if (e.target.value) onLoadFromTable(e.target.value)
							e.target.value = ''
						}}
					>
						<option value="" disabled>
							{tables && tables.length > 0 ? 'Choose a table…' : 'No tables available'}
						</option>
						{tables?.map((t) => (
							<option key={t.tableUniqueId} value={t.tableUniqueId}>
								{t.tableName}
							</option>
						))}
					</select>
				</div>
			</div>

			<KeyFieldSection
				title="Row keys"
				hint="At least one required. Determines how rows are partitioned."
				fields={schema.rowKeys}
				onUpdate={updateRowKey}
				onAdd={() => onChange({ ...schema, rowKeys: [...schema.rowKeys, newKeyField()] })}
				onRemove={(i) => onChange({ ...schema, rowKeys: schema.rowKeys.filter((_, j) => j !== i) })}
				canRemove={schema.rowKeys.length > 1}
			/>

			<KeyFieldSection
				title="Sort keys"
				hint="Optional. Rows within a partition are sorted by these."
				fields={schema.sortKeys}
				onUpdate={updateSortKey}
				onAdd={() => onChange({ ...schema, sortKeys: [...schema.sortKeys, newKeyField()] })}
				onRemove={(i) => onChange({ ...schema, sortKeys: schema.sortKeys.filter((_, j) => j !== i) })}
				canRemove
			/>

			<div className="wizard-schema-section">
				<div className="wizard-schema-section-head">
					<h4>Value fields</h4>
					<button
						className="btn-link"
						onClick={() => onChange({ ...schema, valueFields: [...schema.valueFields, newValueField()] })}
					>
						+ Add field
					</button>
				</div>
				<p className="wizard-schema-hint">Optional. May be primitive, map, or list types, and may be nullable.</p>
				{schema.valueFields.length === 0 && <p className="wizard-schema-empty">No value fields.</p>}
				{schema.valueFields.map((field, i) => (
					<ValueFieldRow
						key={i}
						field={field}
						onUpdate={(f) => updateValueField(i, f)}
						onRemove={() => onChange({ ...schema, valueFields: schema.valueFields.filter((_, j) => j !== i) })}
					/>
				))}
			</div>
		</div>
	)
}

function KeyFieldSection({
	title,
	hint,
	fields,
	onUpdate,
	onAdd,
	onRemove,
	canRemove,
}: {
	title: string
	hint: string
	fields: KeyFieldDraft[]
	onUpdate: (i: number, field: KeyFieldDraft) => void
	onAdd: () => void
	onRemove: (i: number) => void
	canRemove: boolean
}) {
	return (
		<div className="wizard-schema-section">
			<div className="wizard-schema-section-head">
				<h4>{title}</h4>
				<button className="btn-link" onClick={onAdd}>
					+ Add field
				</button>
			</div>
			<p className="wizard-schema-hint">{hint}</p>
			{fields.length === 0 && <p className="wizard-schema-empty">None.</p>}
			{fields.map((field, i) => (
				<div key={i} className="wizard-field-row">
					<input
						className="modal-input wizard-field-name"
						type="text"
						value={field.name}
						placeholder="Field name"
						onChange={(e) => onUpdate(i, { ...field, name: e.target.value })}
					/>
					<select
						className="wizard-field-type"
						value={field.type}
						onChange={(e) => onUpdate(i, { ...field, type: e.target.value as PrimitiveTypeName })}
					>
						{PRIMITIVE_TYPES.map((t) => (
							<option key={t} value={t}>
								{PRIMITIVE_TYPE_LABELS[t]}
							</option>
						))}
					</select>
					<button
						className="btn-link wizard-field-remove"
						onClick={() => onRemove(i)}
						disabled={!canRemove}
						title={canRemove ? 'Remove field' : 'At least one required'}
					>
						Remove
					</button>
				</div>
			))}
		</div>
	)
}

function ValueFieldRow({
	field,
	onUpdate,
	onRemove,
}: {
	field: ValueFieldDraft
	onUpdate: (f: ValueFieldDraft) => void
	onRemove: () => void
}) {
	const kinds: { value: ValueTypeKind; label: string }[] = [
		...PRIMITIVE_TYPES.map((t) => ({ value: t as ValueTypeKind, label: PRIMITIVE_TYPE_LABELS[t] })),
		{ value: 'MapType', label: 'Map' },
		{ value: 'ListType', label: 'List' },
	]
	return (
		<div className="wizard-value-field">
			<div className="wizard-field-row">
				<input
					className="modal-input wizard-field-name"
					type="text"
					value={field.name}
					placeholder="Field name"
					onChange={(e) => onUpdate({ ...field, name: e.target.value })}
				/>
				<select
					className="wizard-field-type"
					value={field.kind}
					onChange={(e) => onUpdate({ ...field, kind: e.target.value as ValueTypeKind })}
				>
					{kinds.map((k) => (
						<option key={k.value} value={k.value}>
							{k.label}
						</option>
					))}
				</select>
				<label className="wizard-field-nullable">
					<input
						type="checkbox"
						checked={field.nullable}
						onChange={(e) => onUpdate({ ...field, nullable: e.target.checked })}
					/>
					nullable
				</label>
				<button className="btn-link wizard-field-remove" onClick={onRemove} title="Remove field">
					Remove
				</button>
			</div>
			{field.kind === 'MapType' && (
				<div className="wizard-field-subrow">
					<span className="wizard-field-sublabel">key</span>
					<select
						value={field.mapKeyType}
						onChange={(e) => onUpdate({ ...field, mapKeyType: e.target.value as PrimitiveTypeName })}
					>
						{PRIMITIVE_TYPES.map((t) => (
							<option key={t} value={t}>
								{PRIMITIVE_TYPE_LABELS[t]}
							</option>
						))}
					</select>
					<span className="wizard-field-sublabel">value</span>
					<select
						value={field.mapValueType}
						onChange={(e) => onUpdate({ ...field, mapValueType: e.target.value as PrimitiveTypeName })}
					>
						{PRIMITIVE_TYPES.map((t) => (
							<option key={t} value={t}>
								{PRIMITIVE_TYPE_LABELS[t]}
							</option>
						))}
					</select>
				</div>
			)}
			{field.kind === 'ListType' && (
				<div className="wizard-field-subrow">
					<span className="wizard-field-sublabel">element</span>
					<select
						value={field.elementType}
						onChange={(e) => onUpdate({ ...field, elementType: e.target.value as PrimitiveTypeName })}
					>
						{PRIMITIVE_TYPES.map((t) => (
							<option key={t} value={t}>
								{PRIMITIVE_TYPE_LABELS[t]}
							</option>
						))}
					</select>
				</div>
			)}
		</div>
	)
}

// ── Step 3: Split points ──

function SplitPointsStep({
	rowKeyType,
	value,
	onChange,
	onLoadExample,
	tables,
	onLoadFromTable,
}: {
	rowKeyType: PrimitiveTypeName | null
	value: string
	onChange: (v: string) => void
	onLoadExample: (id: string) => void
	tables: TableStatus[] | null
	onLoadFromTable: (tableId: string) => void
}) {
	const examples = splitPointExamplesForType(rowKeyType)
	return (
		<div className="wizard-field-block">
			<p className="modal-description">
				Split points pre-partition the table on the first row key. Leave blank to start with a single
				partition. Enter one value per line, in ascending order.
			</p>
			{rowKeyType === 'ByteArrayType' && (
				<p className="modal-note">
					For byte-array row keys, enter each split point as base64.
				</p>
			)}
			<div className="wizard-picker-row">
				{examples.length > 0 && (
					<div className="wizard-template-picker">
						<span className="modal-field-label">Load an example</span>
						<select
							defaultValue=""
							onChange={(e) => {
								if (e.target.value) onLoadExample(e.target.value)
								e.target.value = ''
							}}
						>
							<option value="" disabled>
								Choose an example…
							</option>
							{examples.map((ex) => (
								<option key={ex.id} value={ex.id}>
									{ex.label}
								</option>
							))}
						</select>
					</div>
				)}
				<div className="wizard-template-picker">
					<span className="modal-field-label">Copy from an existing table</span>
					<select
						defaultValue=""
						disabled={!tables || tables.length === 0}
						onChange={(e) => {
							if (e.target.value) onLoadFromTable(e.target.value)
							e.target.value = ''
						}}
					>
						<option value="" disabled>
							{tables && tables.length > 0 ? 'Choose a table…' : 'No tables available'}
						</option>
						{tables?.map((t) => (
							<option key={t.tableUniqueId} value={t.tableUniqueId}>
								{t.tableName}
							</option>
						))}
					</select>
				</div>
			</div>
			<label className="modal-field">
				<span className="modal-field-label">Split points</span>
				<textarea
					className="modal-input wizard-splitpoints"
					value={value}
					onChange={(e) => onChange(e.target.value)}
					rows={8}
					placeholder={'one value per line'}
				/>
			</label>
		</div>
	)
}

// ── Step 4: Review ──

function ReviewStep({
	name,
	schema,
	splitPointsRaw,
}: {
	name: string
	schema: SchemaDraft
	splitPointsRaw: string
}) {
	const splitCount = splitPointsRaw.split('\n').map((l) => l.trim()).filter((l) => l !== '').length
	return (
		<div className="wizard-review">
			<div className="wizard-review-row">
				<span className="modal-field-label">Table name</span>
				<span className="wizard-review-value">{name}</span>
			</div>
			<div className="wizard-review-row">
				<span className="modal-field-label">Split points</span>
				<span className="wizard-review-value">
					{splitCount === 0 ? 'None (single partition)' : `${splitCount} point${splitCount === 1 ? '' : 's'}`}
				</span>
			</div>
			<div className="wizard-review-schema">
				<span className="modal-field-label">Schema</span>
				<pre className="wizard-review-json">{schemaDraftToJsonString(schema, true)}</pre>
			</div>
		</div>
	)
}
