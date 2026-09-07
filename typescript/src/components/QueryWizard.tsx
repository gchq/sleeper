import { useEffect, useState } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { apiFetch, postJson } from '../lib/api'
import { useInstance } from '../contexts/InstanceContext'
import { parseSchemaString, type KeyFieldDraft, type PrimitiveTypeName } from '../lib/tableSchema'
import './QueryWizard.css'

interface Props {
	onClose: () => void
	onSubmitted?: () => void
	presetTableId?: string
}

interface SchemaResponse {
	tableId: string
	tableName: string
	schema: string
}

interface ConditionDraft {
	field: string
	min: string
	minInclusive: boolean
	max: string
	maxInclusive: boolean
	exact: boolean
}

interface SubmitResponse {
	queryId: string
}

const STEPS = ['Table', 'Conditions', 'Values', 'Review'] as const

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

function newCondition(field: string): ConditionDraft {
	return { field, min: '', minInclusive: true, max: '', maxInclusive: false, exact: true }
}

// Validate a single value against its field type. Returns an error message, or null if valid.
function validateValue(value: string, type: PrimitiveTypeName, label: string): string | null {
	if (type === 'IntType' || type === 'LongType') {
		if (!/^-?\d+$/.test(value)) {
			return `${label} must be a whole number.`
		}
		if (type === 'IntType') {
			const n = Number(value)
			if (n < -2147483648 || n > 2147483647) {
				return `${label} is out of range for an Int field.`
			}
		}
	} else if (type === 'ByteArrayType') {
		// Byte array keys are entered as base64.
		if (!/^[A-Za-z0-9+/]*={0,2}$/.test(value) || value.length % 4 !== 0) {
			return `${label} must be base64-encoded for a byte array field.`
		}
	}
	// StringType accepts any non-empty value.
	return null
}

// Compare two values of the given type. Returns negative/zero/positive, or null if incomparable.
function compareValues(a: string, b: string, type: PrimitiveTypeName): number | null {
	if (type === 'IntType' || type === 'LongType') {
		const na = Number(a)
		const nb = Number(b)
		return na === nb ? 0 : na < nb ? -1 : 1
	}
	if (type === 'StringType') {
		return a === b ? 0 : a < b ? -1 : 1
	}
	// Byte arrays (base64) aren't ordered meaningfully as strings here.
	return null
}

export default function QueryWizard({ onClose, onSubmitted, presetTableId }: Props) {
	const { tables } = useInstance()
	const navigate = useNavigate()

	const [step, setStep] = useState(0)
	const [tableId, setTableId] = useState(presetTableId ?? '')
	const [schema, setSchema] = useState<SchemaResponse | null>(null)
	const [rowKeys, setRowKeys] = useState<KeyFieldDraft[]>([])
	const [valueFieldNames, setValueFieldNames] = useState<string[]>([])
	const [conditions, setConditions] = useState<ConditionDraft[]>([])
	const [selectedValueFields, setSelectedValueFields] = useState<string[]>([])

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

	async function loadSchema(id: string): Promise<boolean> {
		setChecking(true)
		setStepError(null)
		try {
			const resp = await apiFetch(`/tables/${encodeURIComponent(id)}/schema`)
			if (!resp.ok) {
				setStepError(await readError(resp))
				return false
			}
			const result: SchemaResponse = await resp.json()
			setSchema(result)
			const draft = parseSchemaString(result.schema)
			setRowKeys(draft.rowKeys)
			setValueFieldNames(draft.valueFields.map((f) => f.name))
			setConditions(draft.rowKeys.length > 0 ? [newCondition(draft.rowKeys[0].name)] : [])
			setSelectedValueFields([])
			return true
		} catch (err) {
			setStepError((err as Error).message)
			return false
		} finally {
			setChecking(false)
		}
	}

	function goBack() {
		setStepError(null)
		setStep((s) => Math.max(0, s - 1))
	}

	async function goNext() {
		setStepError(null)

		if (step === 0) {
			if (!tableId) {
				setStepError('Select a table to query.')
				return
			}
			if (await loadSchema(tableId)) setStep(1)
			return
		}

		if (step === 1) {
			if (conditions.length === 0) {
				setStepError('Add at least one condition.')
				return
			}
			const typeByField = new Map(rowKeys.map((f) => [f.name, f.type]))
			for (const c of conditions) {
				const type = typeByField.get(c.field)
				if (!type) {
					setStepError(`Unknown field "${c.field}".`)
					return
				}
				const min = c.min.trim()
				const max = c.max.trim()
				const label = c.exact ? `Value for "${c.field}"` : `Minimum for "${c.field}"`

				if (min === '') {
					setStepError(`${label} must not be empty.`)
					return
				}
				const minError = validateValue(min, type, label)
				if (minError) {
					setStepError(minError)
					return
				}

				if (!c.exact) {
					if (max === '') {
						setStepError(`Condition on "${c.field}" is a range but has no maximum. Set a maximum or switch it to an exact match.`)
						return
					}
					const maxError = validateValue(max, type, `Maximum for "${c.field}"`)
					if (maxError) {
						setStepError(maxError)
						return
					}
					const cmp = compareValues(min, max, type)
					if (cmp != null && cmp > 0) {
						setStepError(`Minimum for "${c.field}" must not be greater than its maximum.`)
						return
					}
					if (cmp === 0 && !(c.minInclusive && c.maxInclusive)) {
						setStepError(`Range for "${c.field}" is empty: equal min and max must both be inclusive.`)
						return
					}
				}
			}
			setStep(2)
			return
		}

		if (step === 2) {
			setStep(3)
			return
		}
	}

	async function submit(watch: boolean) {
		setSubmitError(null)
		setSubmitting(true)
		try {
			const payloadConditions = conditions.map((c) => ({
				field: c.field,
				min: c.min.trim(),
				minInclusive: c.minInclusive,
				max: c.exact ? null : c.max.trim() || null,
				maxInclusive: c.maxInclusive,
			}))
			const resp = await postJson('/query/submit', {
				tableId,
				conditions: payloadConditions,
				valueFields: selectedValueFields,
			})
			if (resp.status === 201) {
				const result: SubmitResponse = await resp.json()
				onSubmitted?.()
				if (watch) {
					navigate(`/queries/${encodeURIComponent(result.queryId)}`)
					return
				}
				setSuccess(result)
				return
			}
			setSubmitError(await readError(resp))
		} catch (err) {
			setSubmitError((err as Error).message)
		} finally {
			setSubmitting(false)
		}
	}

	function updateCondition(index: number, patch: Partial<ConditionDraft>) {
		setConditions((prev) => prev.map((c, i) => (i === index ? { ...c, ...patch } : c)))
	}

	function addCondition() {
		if (rowKeys.length === 0) return
		setConditions((prev) => [...prev, newCondition(rowKeys[0].name)])
	}

	function removeCondition(index: number) {
		setConditions((prev) => prev.filter((_, i) => i !== index))
	}

	function toggleValueField(name: string) {
		setSelectedValueFields((prev) => (prev.includes(name) ? prev.filter((n) => n !== name) : [...prev, name]))
	}

	if (success) {
		const detailPath = `/queries/${encodeURIComponent(success.queryId)}`
		return (
			<div className="modal-backdrop" onClick={onClose}>
				<div className="modal query-wizard-modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
					<h3 className="modal-title">Query submitted</h3>
					<div className="query-wizard-success">
						<p>
							Your query has been submitted with ID <code>{success.queryId}</code>. It will appear in the list as
							it progresses.
						</p>
					</div>
					<div className="modal-actions">
						<span style={{ flex: 1 }} />
						<button className="btn" onClick={onClose}>
							Close
						</button>
						<Link className="btn btn-primary" to={detailPath} onClick={onClose}>
							View Query
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
				className="modal query-wizard-modal"
				onClick={(e) => e.stopPropagation()}
				role="dialog"
				aria-modal="true"
				onSubmit={(e) => {
					e.preventDefault()
					if (isLastStep) {
						if (!submitting) submit(false)
					} else if (!checking) {
						goNext()
					}
				}}
			>
				<h3 className="modal-title">Query</h3>

				<ol className="query-wizard-steps">
					{STEPS.map((label, i) => (
						<li
							key={label}
							className={i === step ? 'query-wizard-step active' : i < step ? 'query-wizard-step done' : 'query-wizard-step'}
						>
							<span className="query-wizard-step-num">{i + 1}</span>
							<span className="query-wizard-step-label">{label}</span>
						</li>
					))}
				</ol>

				<div className="query-wizard-body">
					{step === 0 && (
						<TableStep
							tableId={tableId}
							onChange={setTableId}
							tables={tables ?? []}
						/>
					)}
					{step === 1 && (
						<ConditionsStep
							rowKeys={rowKeys}
							conditions={conditions}
							onUpdate={updateCondition}
							onAdd={addCondition}
							onRemove={removeCondition}
						/>
					)}
					{step === 2 && (
						<ValuesStep
							valueFieldNames={valueFieldNames}
							selected={selectedValueFields}
							onToggle={toggleValueField}
						/>
					)}
					{step === 3 && (
						<ReviewStep
							tableName={schema?.tableName ?? tableId}
							conditions={conditions.filter((c) => c.min.trim() !== '')}
							valueFields={selectedValueFields}
						/>
					)}
				</div>

				<div className="query-wizard-footer">
					{stepError && <p className="error modal-error">{stepError}</p>}
					{isLastStep && submitError && <p className="error modal-error">Submit failed: {submitError}</p>}

					<div className="modal-actions">
						{!isLastStep && (
							<button type="submit" className="btn btn-primary" style={{ order: 4 }} disabled={checking}>
								{checking ? 'Loading…' : 'Next'}
							</button>
						)}
						{isLastStep && (
							<>
								<button type="submit" className="btn" style={{ order: 4 }} disabled={submitting}>
									{submitting ? 'Submitting…' : 'Submit'}
								</button>
								<button
									type="button"
									className="btn btn-primary"
									style={{ order: 5 }}
									onClick={() => submit(true)}
									disabled={submitting}
								>
									{submitting ? 'Submitting…' : 'Submit & Watch'}
								</button>
							</>
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

function TableStep({
	tableId,
	onChange,
	tables,
}: {
	tableId: string
	onChange: (id: string) => void
	tables: { tableUniqueId: string; tableName: string }[]
}) {
	return (
		<div className="query-wizard-field-block">
			<p className="modal-description">Choose the Sleeper table to query.</p>
			<label className="modal-field">
				<span className="modal-field-label">Table</span>
				<select className="modal-input" value={tableId} onChange={(e) => onChange(e.target.value)} autoFocus>
					<option value="" disabled>
						Select a table…
					</option>
					{tables.map((t) => (
						<option key={t.tableUniqueId} value={t.tableUniqueId}>
							{t.tableName}
						</option>
					))}
				</select>
			</label>
		</div>
	)
}

function ConditionsStep({
	rowKeys,
	conditions,
	onUpdate,
	onAdd,
	onRemove,
}: {
	rowKeys: KeyFieldDraft[]
	conditions: ConditionDraft[]
	onUpdate: (index: number, patch: Partial<ConditionDraft>) => void
	onAdd: () => void
	onRemove: (index: number) => void
}) {
	return (
		<div className="query-wizard-field-block">
			<p className="modal-description">
				Add one or more conditions on the table's row key fields. Use an exact match for a single value, or a range
				with a minimum and maximum.
			</p>
			{rowKeys.length === 0 ? (
				<p className="query-wizard-empty">This table has no row key fields to query on.</p>
			) : (
				<>
					<ul className="query-wizard-conditions">
						{conditions.map((c, i) => (
							<li key={i} className="query-wizard-condition">
								<div className="query-wizard-condition-row">
									<select
										className="modal-input query-wizard-field-select"
										value={c.field}
										onChange={(e) => onUpdate(i, { field: e.target.value })}
										aria-label="Field"
									>
										{rowKeys.map((f) => (
											<option key={f.name} value={f.name}>
												{f.name} ({f.type})
											</option>
										))}
									</select>
									<label className="query-wizard-exact">
										<input
											type="checkbox"
											checked={c.exact}
											onChange={(e) => onUpdate(i, { exact: e.target.checked })}
										/>
										Exact match
									</label>
									{conditions.length > 1 && (
										<button type="button" className="btn-link query-wizard-remove" onClick={() => onRemove(i)}>
											Remove
										</button>
									)}
								</div>
								<div className="query-wizard-condition-values">
									<label className="modal-field query-wizard-value">
										<span className="modal-field-label">{c.exact ? 'Value' : 'Minimum'}</span>
										<input
											className="modal-input"
											value={c.min}
											onChange={(e) => onUpdate(i, { min: e.target.value })}
											placeholder={c.exact ? 'Exact value' : 'Min'}
										/>
									</label>
									{!c.exact && (
										<label className="modal-field query-wizard-value">
											<span className="modal-field-label">Maximum</span>
											<input
												className="modal-input"
												value={c.max}
												onChange={(e) => onUpdate(i, { max: e.target.value })}
												placeholder="Max"
											/>
										</label>
									)}
								</div>
								{!c.exact && (
									<div className="query-wizard-inclusive">
										<label>
											<input
												type="checkbox"
												checked={c.minInclusive}
												onChange={(e) => onUpdate(i, { minInclusive: e.target.checked })}
											/>
											Min inclusive
										</label>
										<label>
											<input
												type="checkbox"
												checked={c.maxInclusive}
												onChange={(e) => onUpdate(i, { maxInclusive: e.target.checked })}
											/>
											Max inclusive
										</label>
									</div>
								)}
							</li>
						))}
					</ul>
					<button type="button" className="btn query-wizard-add" onClick={onAdd}>
						Add condition
					</button>
				</>
			)}
		</div>
	)
}

function ValuesStep({
	valueFieldNames,
	selected,
	onToggle,
}: {
	valueFieldNames: string[]
	selected: string[]
	onToggle: (name: string) => void
}) {
	return (
		<div className="query-wizard-field-block">
			<p className="modal-description">
				Choose which value fields to return. Leave all unselected to return every field.
			</p>
			{valueFieldNames.length === 0 ? (
				<p className="query-wizard-empty">This table has no value fields.</p>
			) : (
				<ul className="query-wizard-values-list">
					{valueFieldNames.map((name) => (
						<li key={name}>
							<label className="query-wizard-check">
								<input type="checkbox" checked={selected.includes(name)} onChange={() => onToggle(name)} />
								<span>{name}</span>
							</label>
						</li>
					))}
				</ul>
			)}
		</div>
	)
}

function ReviewStep({
	tableName,
	conditions,
	valueFields,
}: {
	tableName: string
	conditions: ConditionDraft[]
	valueFields: string[]
}) {
	return (
		<div className="query-wizard-review">
			<div className="query-wizard-review-row">
				<span className="modal-field-label">Table</span>
				<span className="query-wizard-review-value">{tableName}</span>
			</div>
			<div className="query-wizard-review-row">
				<span className="modal-field-label">Conditions</span>
				<ul className="query-wizard-review-conditions">
					{conditions.map((c, i) => (
						<li key={i}>
							<code>{c.field}</code>{' '}
							{c.exact
								? `= ${c.min}`
								: `${c.minInclusive ? '[' : '('}${c.min}, ${c.max || '∞'}${c.maxInclusive ? ']' : ')'}`}
						</li>
					))}
				</ul>
			</div>
			<div className="query-wizard-review-row">
				<span className="modal-field-label">Value fields</span>
				<span className="query-wizard-review-value">
					{valueFields.length === 0 ? 'All fields' : valueFields.join(', ')}
				</span>
			</div>
			<p className="modal-note">Results will be written to the query results S3 bucket.</p>
		</div>
	)
}
