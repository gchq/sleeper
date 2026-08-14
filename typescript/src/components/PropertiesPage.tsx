import { useCallback, useEffect, useMemo, useState } from 'react'
import { useBlocker } from 'react-router-dom'
import { useApi } from '../hooks/useApi'
import './PropertiesPage.css'

export interface PropertiesPageAdapter {
	title: string
	definitionsPath: string
	valuesPath: string
	validatePath: string
	savePath: string
}

interface PropertyDefinition {
	name: string
	description: string
	defaultValue: string
	isEditable: boolean
	isRunCdkDeployWhenChanged: boolean
}

interface PropertyGroup {
	description: string
	properties: PropertyDefinition[]
}

type PropertiesResponse = Record<string, PropertyGroup>

type View = 'table' | 'text'

type ValueSource = 'pending' | 'set' | 'default' | 'unset'

interface EffectiveValue {
	text: string
	source: ValueSource
}

interface PendingChange {
	name: string
	baseValue: string
	baseSource: ValueSource
	newValue: string
	addedAt: number
}

type Basket = Record<string, PendingChange>

interface Conflict {
	change: PendingChange
	current: EffectiveValue
}

interface InvalidPropertyDetail {
	name: string
	value: string
	reason: string
}

interface UpdateFailureBody {
	message?: string
	invalidProperties?: InvalidPropertyDetail[]
	nonEditableProperties?: string[]
	cdkDeployRequiredProperties?: string[]
	unknownProperties?: string[]
}

interface ApplyError {
	message: string
	details?: UpdateFailureBody
}

async function parseApplyError(resp: Response): Promise<ApplyError> {
	const fallback = `HTTP ${resp.status}`
	try {
		const body = await resp.json()
		if (body && typeof body === 'object') {
			const details = body as UpdateFailureBody
			const message = typeof details.message === 'string' && details.message ? details.message : fallback
			return { message, details }
		}
	} catch {
		// response body was not JSON; fall through to the plain status message
	}
	return { message: fallback }
}

const TEXT_LINE_WIDTH = 100

function effectiveWithoutBasket(prop: PropertyDefinition, values: Record<string, string> | null): EffectiveValue {
	const set = values?.[prop.name]
	if (typeof set === 'string' && set !== '') return { text: set, source: 'set' }
	if (typeof prop.defaultValue === 'string' && prop.defaultValue !== '') {
		return { text: prop.defaultValue, source: 'default' }
	}
	return { text: '', source: 'unset' }
}

function effectiveValue(
	prop: PropertyDefinition,
	values: Record<string, string> | null,
	basket: Basket,
): EffectiveValue {
	const pending = basket[prop.name]
	if (pending) return { text: pending.newValue, source: 'pending' }
	return effectiveWithoutBasket(prop, values)
}

function wrapAsComment(text: string, width = TEXT_LINE_WIDTH): string[] {
	const lines: string[] = []
	for (const paragraph of text.split(/\r?\n/)) {
		if (paragraph === '') {
			lines.push('#')
			continue
		}
		const words = paragraph.split(/\s+/).filter(Boolean)
		let cur = ''
		for (const word of words) {
			if (!cur) {
				cur = word
			} else if (`# ${cur} ${word}`.length > width) {
				lines.push(`# ${cur}`)
				cur = word
			} else {
				cur += ' ' + word
			}
		}
		if (cur) lines.push(`# ${cur}`)
	}
	return lines
}

function bannerLines(title: string, width = 81): string[] {
	const bar = '#'.repeat(width)
	const inner = width - 2
	const pad = Math.max(0, inner - title.length)
	const left = Math.floor(pad / 2)
	const right = pad - left
	return [bar, `#${' '.repeat(left)}${title}${' '.repeat(right)}#`, bar]
}

type TextLine = { text: string; pending?: boolean }

function renderTemplateLines(
	filtered: PropertiesResponse,
	values: Record<string, string> | null,
	basket: Basket,
	showDescriptions: boolean,
	bannerTitle: string,
): TextLine[] {
	const out: TextLine[] = []
	const push = (text: string, pending = false) => out.push({ text, pending })
	if (showDescriptions) {
		for (const line of bannerLines(bannerTitle)) push(line)
	}
	let first = true
	for (const [groupName, group] of Object.entries(filtered)) {
		if (showDescriptions) {
			push('')
			push('')
			for (const line of bannerLines(groupName.toUpperCase())) push(line)
			push('')
			if (group.description) {
				for (const line of wrapAsComment(group.description)) push('#' + line)
				push('')
			}
		} else if (!first) {
			push('')
		}
		first = false
		for (const p of group.properties) {
			if (showDescriptions && p.description) {
				for (const line of wrapAsComment(p.description)) push(line)
			}
			const pending = basket[p.name]
			if (pending) {
				const base = pending.baseSource === 'unset' ? '(unset)' : pending.baseValue
				push(`# (pending change: was ${base})`, true)
				push(`${p.name}=${pending.newValue}`, true)
			} else {
				const set = values?.[p.name]
				const hasSet = typeof set === 'string' && set !== ''
				const def = p.defaultValue
				const hasDefault = typeof def === 'string' && def !== ''
				if (hasSet) {
					push(`${p.name}=${set}`)
				} else if (hasDefault) {
					if (showDescriptions) {
						push('# (default value shown below, uncomment to set a value)')
					}
					push(`# ${p.name}=${def}`)
				} else {
					push('# (uncomment to set a value)')
					push(`# ${p.name}=`)
				}
			}
			if (showDescriptions) push('')
		}
	}
	return out
}

interface DescriptionModalProps {
	prop: PropertyDefinition
	onClose: () => void
}

function DescriptionModal({ prop, onClose }: DescriptionModalProps) {
	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onClose()
		}
		window.addEventListener('keydown', onKey)
		return () => window.removeEventListener('keydown', onKey)
	}, [onClose])

	return (
		<div className="modal-backdrop" onClick={onClose}>
			<div className="modal desc-modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
				<h3 className="modal-title">{prop.name}</h3>
				<p className="modal-description">{prop.description || <em>No description.</em>}</p>
				{prop.defaultValue && (
					<div className="modal-meta">
						<span>
							Default: <code>{prop.defaultValue}</code>
						</span>
					</div>
				)}
				<div className="modal-actions">
					<span style={{ flex: 1 }} />
					<button className="btn" onClick={onClose}>
						Close
					</button>
				</div>
			</div>
		</div>
	)
}

interface EditModalProps {
	prop: PropertyDefinition
	initialValue: string
	initialSource: ValueSource
	currentValue: string
	isStaged: boolean
	validatePath: string
	onConfirm: (newValue: string) => void
	onUnstage: () => void
	onCancel: () => void
}

function EditPropertyModal({
	prop,
	initialValue,
	initialSource,
	currentValue,
	isStaged,
	validatePath,
	onConfirm,
	onUnstage,
	onCancel,
}: EditModalProps) {
	const [value, setValue] = useState(initialValue)
	const [validating, setValidating] = useState(false)
	const [validationError, setValidationError] = useState<string | null>(null)
	const isMultiline = value.includes('\n') || value.length > 80

	const matchesInitial = value === initialValue
	const matchesCurrent = value === currentValue
	const unstageMode = isStaged && (matchesInitial || matchesCurrent)
	const stageDisabled = matchesCurrent
	const submitDisabled = validating || (!unstageMode && stageDisabled)

	function reset() {
		setValue(prop.defaultValue ?? '')
		setValidationError(null)
	}

	function updateValue(v: string) {
		setValue(v)
		if (validationError) setValidationError(null)
	}

	async function submit() {
		if (unstageMode) {
			onUnstage()
			return
		}
		setValidating(true)
		setValidationError(null)
		try {
			const resp = await fetch('/api' + validatePath, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ name: prop.name, value }),
			})
			if (!resp.ok) {
				setValidationError(`Validation request failed: HTTP ${resp.status}`)
				return
			}
			const result: { valid: boolean; reason?: string | null } = await resp.json()
			if (!result.valid) {
				setValidationError(result.reason || 'Value did not pass validation.')
				return
			}
			onConfirm(value)
		} catch (err) {
			setValidationError((err as Error).message)
		} finally {
			setValidating(false)
		}
	}

	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onCancel()
		}
		window.addEventListener('keydown', onKey)
		return () => window.removeEventListener('keydown', onKey)
	}, [onCancel])

	return (
		<div className="modal-backdrop" onClick={onCancel}>
			<form
				className="modal edit-modal"
				onClick={(e) => e.stopPropagation()}
				role="dialog"
				aria-modal="true"
				onSubmit={(e) => {
					e.preventDefault()
					if (!submitDisabled) submit()
				}}
			>
				<h3 className="modal-title">{prop.name}</h3>
				<p className="modal-description">{prop.description || <em>No description.</em>}</p>
				<div className="modal-meta">
					<span>
						Current source: <strong>{initialSource}</strong>
					</span>
					{prop.defaultValue && (
						<span>
							Default: <code>{prop.defaultValue}</code>
						</span>
					)}
				</div>
				<label className="modal-field">
					<span className="modal-field-label">Value</span>
					{isMultiline ? (
						<textarea
							className="modal-input"
							value={value}
							onChange={(e) => updateValue(e.target.value)}
							rows={Math.min(10, Math.max(3, value.split('\n').length))}
							autoFocus
						/>
					) : (
						<input
							className="modal-input"
							type="text"
							value={value}
							onChange={(e) => updateValue(e.target.value)}
							autoFocus
						/>
					)}
				</label>
				{validationError && <p className="error modal-error">{validationError}</p>}
				<div className="modal-actions">
					<button
						type="button"
						className="btn"
						onClick={reset}
						disabled={validating || value === (prop.defaultValue ?? '')}
					>
						Reset to default
					</button>
					<span style={{ flex: 1 }} />
					<button type="button" className="btn" onClick={onCancel} disabled={validating}>
						Cancel
					</button>
					<button
						type="submit"
						className={unstageMode ? 'btn btn-danger' : 'btn btn-primary'}
						disabled={submitDisabled}
					>
						{validating ? 'Validating…' : unstageMode ? 'Unstage Change' : 'Stage Change'}
					</button>
				</div>
			</form>
		</div>
	)
}

interface ConfirmLeaveModalProps {
	count: number
	onDiscard: () => void
	onStay: () => void
}

function ConfirmLeaveModal({ count, onDiscard, onStay }: ConfirmLeaveModalProps) {
	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onStay()
		}
		window.addEventListener('keydown', onKey)
		return () => window.removeEventListener('keydown', onKey)
	}, [onStay])

	return (
		<div className="modal-backdrop" onClick={onStay}>
			<div className="modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
				<h3 className="modal-title">Discard pending changes?</h3>
				<p className="modal-description">
					You have {count} pending {count === 1 ? 'change' : 'changes'} that have not been applied. If you leave this page, these changes will be lost.
				</p>
				<div className="modal-actions">
					<span style={{ flex: 1 }} />
					<button className="btn" onClick={onStay} autoFocus>
						Stay on page
					</button>
					<button className="btn btn-danger" onClick={onDiscard}>
						Discard and leave
					</button>
				</div>
			</div>
		</div>
	)
}

function ApplyErrorDetails({ details }: { details: UpdateFailureBody }) {
	const { invalidProperties, unknownProperties, nonEditableProperties, cdkDeployRequiredProperties } = details
	const hasInvalid = invalidProperties && invalidProperties.length > 0
	const hasUnknown = unknownProperties && unknownProperties.length > 0
	const hasNonEditable = nonEditableProperties && nonEditableProperties.length > 0
	const hasCdk = cdkDeployRequiredProperties && cdkDeployRequiredProperties.length > 0
	if (!hasInvalid && !hasUnknown && !hasNonEditable && !hasCdk) return null
	return (
		<div className="apply-error-details">
			{hasInvalid && (
				<div className="apply-error-section">
					<strong>Invalid values:</strong>
					<ul>
						{invalidProperties!.map((p) => (
							<li key={p.name}>
								<code>{p.name}</code> = <code>{p.value}</code>
								{p.reason ? <>: {p.reason}</> : null}
							</li>
						))}
					</ul>
				</div>
			)}
			{hasUnknown && (
				<div className="apply-error-section">
					<strong>Unknown properties:</strong> <code>{unknownProperties!.join(', ')}</code>
				</div>
			)}
			{hasNonEditable && (
				<div className="apply-error-section">
					<strong>Non-editable properties:</strong> <code>{nonEditableProperties!.join(', ')}</code>
				</div>
			)}
			{hasCdk && (
				<div className="apply-error-section">
					<strong>Require CDK redeploy:</strong> <code>{cdkDeployRequiredProperties!.join(', ')}</code>
				</div>
			)}
		</div>
	)
}

interface BasketModalProps {
	basket: Basket
	propsByName: Record<string, PropertyDefinition>
	values: Record<string, string> | null
	applying: boolean
	applyError: ApplyError | null
	conflicts: Conflict[]
	onRemove: (name: string) => void
	onKeepPending: (name: string) => void
	onAdoptCurrent: (name: string) => void
	onClose: () => void
	onApply: () => void
}

function BasketModal({
	basket,
	propsByName,
	values,
	applying,
	applyError,
	conflicts,
	onRemove,
	onKeepPending,
	onAdoptCurrent,
	onClose,
	onApply,
}: BasketModalProps) {
	const items = Object.values(basket).sort((a, b) => a.addedAt - b.addedAt)
	const conflictsByName = useMemo(() => {
		const m: Record<string, Conflict> = {}
		for (const c of conflicts) m[c.change.name] = c
		return m
	}, [conflicts])

	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onClose()
		}
		window.addEventListener('keydown', onKey)
		return () => window.removeEventListener('keydown', onKey)
	}, [onClose])

	return (
		<div className="modal-backdrop" onClick={onClose}>
			<div className="modal basket-modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
				<h3 className="modal-title">Pending changes ({items.length})</h3>
				{items.length === 0 ? (
					<p className="modal-empty">No pending changes.</p>
				) : (
					<ul className="basket-list">
						{items.map((change) => {
							const prop = propsByName[change.name]
							const conflict = conflictsByName[change.name]
							return (
								<li key={change.name} className={conflict ? 'basket-item basket-item-conflict' : 'basket-item'}>
									<div className="basket-item-header">
										<span className="basket-item-name">{change.name}</span>
										<button className="btn-link" onClick={() => onRemove(change.name)}>
											Remove
										</button>
									</div>
									{prop?.description && <p className="basket-item-desc">{prop.description}</p>}
									<div className="basket-item-diff">
										<span className="basket-item-from">
											was: <code>{change.baseSource === 'unset' ? '(unset)' : change.baseValue}</code>
											{change.baseSource === 'default' && <span className="prop-value-tag">default</span>}
										</span>
										<span className="basket-item-arrow">→</span>
										<span className="basket-item-to">
											new: <code>{change.newValue}</code>
										</span>
									</div>
									{conflict && (
										<div className="basket-item-warning">
											<strong>⚠ Value has changed since you added this.</strong>
											<div>
												Current value: <code>{conflict.current.source === 'unset' ? '(unset)' : conflict.current.text}</code>
												{conflict.current.source === 'default' && <span className="prop-value-tag">default</span>}
											</div>
											<div className="basket-item-warning-actions">
												<button className="btn" onClick={() => onKeepPending(change.name)}>
													Keep my change ({change.newValue})
												</button>
												<button className="btn" onClick={() => onAdoptCurrent(change.name)}>
													Discard (accept new current value)
												</button>
											</div>
										</div>
									)}
								</li>
							)
						})}
					</ul>
				)}
				{applyError && (
					<div className="apply-error">
						<p className="error">Apply failed: {applyError.message}</p>
						{applyError.details && <ApplyErrorDetails details={applyError.details} />}
					</div>
				)}
				<div className="modal-actions">
					<button className="btn" onClick={onClose} disabled={applying}>
						Close
					</button>
					<span style={{ flex: 1 }} />
					<button
						className="btn btn-primary"
						onClick={onApply}
						disabled={applying || items.length === 0 || conflicts.length > 0}
					>
						{applying ? 'Applying...' : conflicts.length > 0 ? 'Resolve conflicts to apply' : `Apply ${items.length}`}
					</button>
				</div>
				{propsByName && !values && (
					<p className="modal-note">Live values are unavailable; conflict detection may be limited.</p>
				)}
			</div>
		</div>
	)
}

function withReloadKey(path: string, key: number): string {
	if (key === 0) return path
	return path.includes('?') ? `${path}&_=${key}` : `${path}?_=${key}`
}

export default function PropertiesPage({ adapter }: { adapter: PropertiesPageAdapter }) {
	const [reloadKey, setReloadKey] = useState(0)
	const { data, error } = useApi<PropertiesResponse>(adapter.definitionsPath)
	const { data: values } = useApi<Record<string, string>>(withReloadKey(adapter.valuesPath, reloadKey))
	const [search, setSearch] = useState('')
	const [onlyWithValue, setOnlyWithValue] = useState(false)
	const [onlyNonDefault, setOnlyNonDefault] = useState(false)
	const [onlyEditable, setOnlyEditable] = useState(false)
	const [showDescriptions, setShowDescriptions] = useState(true)
	const [view, setView] = useState<View>('table')
	const [collapsed, setCollapsed] = useState<Set<string>>(new Set())
	const [basket, setBasket] = useState<Basket>({})
	const [editing, setEditing] = useState<{
		prop: PropertyDefinition
		initialValue: string
		initialSource: ValueSource
		currentValue: string
		isStaged: boolean
	} | null>(null)
	const [showBasket, setShowBasket] = useState(false)
	const [describing, setDescribing] = useState<PropertyDefinition | null>(null)
	const [conflicts, setConflicts] = useState<Conflict[]>([])
	const [applying, setApplying] = useState(false)
	const [applyError, setApplyError] = useState<ApplyError | null>(null)

	// Clear basket / conflicts when the adapter (page context) changes.
	const adapterKey = `${adapter.definitionsPath}|${adapter.valuesPath}|${adapter.savePath}`
	useEffect(() => {
		setBasket({})
		setConflicts([])
		setApplyError(null)
		setReloadKey(0)
	}, [adapterKey])

	const hasPending = Object.keys(basket).length > 0

	const blocker = useBlocker(hasPending)

	useEffect(() => {
		if (!hasPending) return
		function onBeforeUnload(e: BeforeUnloadEvent) {
			e.preventDefault()
		}
		window.addEventListener('beforeunload', onBeforeUnload)
		return () => window.removeEventListener('beforeunload', onBeforeUnload)
	}, [hasPending])

	const propsByName = useMemo(() => {
		const m: Record<string, PropertyDefinition> = {}
		if (!data) return m
		for (const group of Object.values(data)) {
			for (const p of group.properties) m[p.name] = p
		}
		return m
	}, [data])

	const filtered = useMemo(() => {
		if (!data) return null
		const term = search.trim().toLowerCase()
		const result: PropertiesResponse = {}
		for (const [component, group] of Object.entries(data)) {
			const matching = group.properties.filter((p) => {
				const setValue = values?.[p.name]
				const hasSet = typeof setValue === 'string' && setValue !== ''
				const hasDefault = typeof p.defaultValue === 'string' && p.defaultValue !== ''
				if (term) {
					const pending = basket[p.name]?.newValue ?? ''
					const haystack = `${p.name ?? ''}\n${p.description ?? ''}\n${p.defaultValue ?? ''}\n${setValue ?? ''}\n${pending}`.toLowerCase()
					if (!haystack.includes(term)) return false
				}
				if (onlyWithValue && !hasSet && !hasDefault && !basket[p.name]) return false
				if (onlyNonDefault && (!hasSet || setValue === p.defaultValue) && !basket[p.name]) return false
				if (onlyEditable && !p.isEditable) return false
				return true
			})
			if (matching.length > 0) {
				result[component] = { description: group.description, properties: matching }
			}
		}
		return result
	}, [data, values, basket, search, onlyWithValue, onlyNonDefault, onlyEditable])

	const openEdit = useCallback(
		(prop: PropertyDefinition) => {
			const eff = effectiveValue(prop, values, basket)
			const current = effectiveWithoutBasket(prop, values)
			setEditing({
				prop,
				initialValue: eff.text,
				initialSource: eff.source,
				currentValue: current.text,
				isStaged: !!basket[prop.name],
			})
		},
		[values, basket],
	)

	const confirmEdit = useCallback(
		(newValue: string) => {
			if (!editing) return
			const base = effectiveWithoutBasket(editing.prop, values)
			setBasket((b) => ({
				...b,
				[editing.prop.name]: {
					name: editing.prop.name,
					baseValue: base.text,
					baseSource: base.source,
					newValue,
					addedAt: b[editing.prop.name]?.addedAt ?? Date.now(),
				},
			}))
			setEditing(null)
		},
		[editing, values],
	)

	const removeFromBasket = useCallback((name: string) => {
		setBasket((b) => {
			const next = { ...b }
			delete next[name]
			return next
		})
		setConflicts((cs) => cs.filter((c) => c.change.name !== name))
	}, [])

	const keepPending = useCallback(
		(name: string) => {
			setConflicts((cs) => cs.filter((c) => c.change.name !== name))
			setBasket((b) => {
				const existing = b[name]
				const prop = propsByName[name]
				if (!existing || !prop) return b
				const current = effectiveWithoutBasket(prop, values)
				return {
					...b,
					[name]: { ...existing, baseValue: current.text, baseSource: current.source },
				}
			})
		},
		[values, propsByName],
	)

	const adoptCurrent = useCallback((name: string) => {
		removeFromBasket(name)
	}, [removeFromBasket])

	function detectConflicts(): Conflict[] {
		const found: Conflict[] = []
		for (const change of Object.values(basket)) {
			const prop = propsByName[change.name]
			if (!prop) continue
			const current = effectiveWithoutBasket(prop, values)
			if (current.text !== change.baseValue || current.source !== change.baseSource) {
				found.push({ change, current })
			}
		}
		return found
	}

	async function applyBasket() {
		const stale = detectConflicts()
		if (stale.length > 0) {
			setConflicts(stale)
			return
		}
		setApplying(true)
		setApplyError(null)
		try {
			const payload: Record<string, string> = {}
			for (const c of Object.values(basket)) payload[c.name] = c.newValue
			const resp = await fetch('/api' + adapter.savePath, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(payload),
			})
			if (!resp.ok) {
				setApplyError(await parseApplyError(resp))
				return
			}
			setBasket({})
			setConflicts([])
			setShowBasket(false)
			setReloadKey((k) => k + 1)
		} catch (err) {
			setApplyError({ message: (err as Error).message })
		} finally {
			setApplying(false)
		}
	}

	function toggleCollapsed(component: string) {
		setCollapsed((prev) => {
			const next = new Set(prev)
			if (next.has(component)) next.delete(component)
			else next.add(component)
			return next
		})
	}

	function collapseAll() {
		if (!filtered) return
		setCollapsed(new Set(Object.keys(filtered)))
	}

	function expandAll() {
		setCollapsed(new Set())
	}

	if (error) {
		return (
			<div className="page">
				<h2>{adapter.title}</h2>
				<p className="error">Failed to load properties: {error}</p>
			</div>
		)
	}

	if (!filtered) {
		return (
			<div className="page">
				<h2>{adapter.title}</h2>
				<p>Loading...</p>
			</div>
		)
	}

	const componentNames = Object.keys(filtered)
	const totalProps = Object.values(filtered).reduce((sum, group) => sum + group.properties.length, 0)
	const basketCount = Object.keys(basket).length
	const bannerTitle = `SLEEPER ${adapter.title.toUpperCase()}`

	return (
		<div className="page">
			<h2>{adapter.title}</h2>
			<div className="toolbar">
				<div className="toolbar-row toolbar-row-primary">
					<input
						className="search-input"
						type="search"
						placeholder="Search properties..."
						value={search}
						onChange={(e) => setSearch(e.target.value)}
						aria-label="Search properties"
					/>
					<button
						className={basketCount > 0 ? 'btn basket-btn basket-btn-active' : 'btn basket-btn'}
						onClick={() => setShowBasket(true)}
						disabled={basketCount === 0}
					>
						Changes ({basketCount})
					</button>
					<div className="view-toggle" role="tablist" aria-label="View mode">
						<button
							role="tab"
							aria-selected={view === 'table'}
							aria-label="Table view"
							title="Table view"
							className={view === 'table' ? 'view-toggle-btn active' : 'view-toggle-btn'}
							onClick={() => setView('table')}
						>
							<svg
								aria-hidden="true"
								width="16"
								height="16"
								viewBox="0 0 16 16"
								fill="none"
								stroke="currentColor"
								strokeWidth="1.5"
								strokeLinecap="round"
								strokeLinejoin="round"
							>
								<rect x="2" y="3" width="12" height="10" rx="1" />
								<path d="M2 7h12" />
								<path d="M2 10h12" />
								<path d="M6 3v10" />
							</svg>
						</button>
						<button
							role="tab"
							aria-selected={view === 'text'}
							aria-label="Text view"
							title="Text view"
							className={view === 'text' ? 'view-toggle-btn active' : 'view-toggle-btn'}
							onClick={() => setView('text')}
						>
							<svg
								aria-hidden="true"
								width="16"
								height="16"
								viewBox="0 0 16 16"
								fill="none"
								stroke="currentColor"
								strokeWidth="1.5"
								strokeLinecap="round"
								strokeLinejoin="round"
							>
								<path d="M3.5 2.5h6l3 3v8a1 1 0 0 1-1 1h-8a1 1 0 0 1-1-1v-10a1 1 0 0 1 1-1z" />
								<path d="M9.5 2.5v3h3" />
								<path d="M5.5 8.5h5" />
								<path d="M5.5 10.5h5" />
								<path d="M5.5 12.5h3" />
							</svg>
						</button>
					</div>
				</div>
				<div className="toolbar-row toolbar-row-secondary">
					<div className="filter-group" role="group" aria-label="Filters">
						<span className="filter-group-label">Filter</span>
						<label className="filter-toggle">
							<input
								type="checkbox"
								checked={onlyWithValue}
								onChange={(e) => setOnlyWithValue(e.target.checked)}
							/>
							Has a value
						</label>
						<label className="filter-toggle">
							<input
								type="checkbox"
								checked={onlyNonDefault}
								onChange={(e) => setOnlyNonDefault(e.target.checked)}
							/>
							Non-default
						</label>
						<label className="filter-toggle">
							<input
								type="checkbox"
								checked={onlyEditable}
								onChange={(e) => setOnlyEditable(e.target.checked)}
							/>
							Editable
						</label>
					</div>
					<label className="filter-toggle">
						<input
							type="checkbox"
							checked={showDescriptions}
							onChange={(e) => setShowDescriptions(e.target.checked)}
						/>
						Show descriptions
					</label>
					<span className="result-count">
						{totalProps} {totalProps === 1 ? 'property' : 'properties'} across {componentNames.length}{' '}
						{componentNames.length === 1 ? 'component' : 'components'}
					</span>
					{view === 'table' && (
						<div className="toolbar-actions">
							<button
								className="btn-link icon-btn"
								onClick={expandAll}
								aria-label="Expand all"
								title="Expand all"
							>
								<svg
									aria-hidden="true"
									width="16"
									height="16"
									viewBox="0 0 16 16"
									fill="none"
									stroke="currentColor"
									strokeWidth="1.5"
									strokeLinecap="round"
									strokeLinejoin="round"
								>
									<path d="M5 5l3-3 3 3" />
									<path d="M5 11l3 3 3-3" />
								</svg>
							</button>
							<button
								className="btn-link icon-btn"
								onClick={collapseAll}
								aria-label="Collapse all"
								title="Collapse all"
							>
								<svg
									aria-hidden="true"
									width="16"
									height="16"
									viewBox="0 0 16 16"
									fill="none"
									stroke="currentColor"
									strokeWidth="1.5"
									strokeLinecap="round"
									strokeLinejoin="round"
								>
									<path d="M5 3l3 3 3-3" />
									<path d="M5 13l3-3 3 3" />
								</svg>
							</button>
						</div>
					)}
				</div>
			</div>

			{componentNames.length === 0 && <p className="no-results">No properties match your search.</p>}

			{view === 'text' && componentNames.length > 0 && (
				<div className="properties-text">
					{renderTemplateLines(filtered, values, basket, showDescriptions, bannerTitle).map((line, i) => (
						<div key={i} className={line.pending ? 'text-line pending-line' : 'text-line'}>
							{line.text || ' '}
						</div>
					))}
				</div>
			)}

			{view === 'table' &&
				componentNames.map((component) => {
					const group = filtered[component]
					const isCollapsed = collapsed.has(component)
					return (
						<section key={component} className="component-section">
							<button
								className="component-header"
								onClick={() => toggleCollapsed(component)}
								aria-expanded={!isCollapsed}
							>
								<span className="component-name">{component}</span>
								<span className="component-count">{group.properties.length}</span>
								<span className="collapse-icon">{isCollapsed ? '▶' : '▼'}</span>
							</button>

							{!isCollapsed && (
								<table className={showDescriptions ? 'properties-table' : 'properties-table no-desc'}>
									<thead>
										<tr>
											<th>Property</th>
											{showDescriptions && <th>Description</th>}
											<th>Value</th>
										</tr>
									</thead>
									<tbody>
										{group.properties.map((prop) => {
											const pending = basket[prop.name]
											const setValue = values?.[prop.name]
											const hasSet = typeof setValue === 'string' && setValue !== ''
											const hasDefault = typeof prop.defaultValue === 'string' && prop.defaultValue !== ''
											let valueClass = 'prop-value prop-value-default'
											let display = prop.defaultValue
											let tag: 'default' | 'pending' | null = hasDefault ? 'default' : null
											if (pending) {
												valueClass = 'prop-value prop-value-pending'
												display = pending.newValue
												tag = 'pending'
											} else if (hasSet) {
												valueClass = 'prop-value prop-value-set'
												display = setValue as string
												tag = null
											}
											return (
												<tr key={prop.name}>
													<td className="prop-name">
														<div className="prop-name-row">
															<span>{prop.name}</span>
															{!showDescriptions && prop.description && (
																<button
																	type="button"
																	className="prop-help"
																	aria-label={`Show description for ${prop.name}`}
																	title={prop.description}
																	onClick={() => setDescribing(prop)}
																>
																	?
																</button>
															)}
														</div>
														{showDescriptions && prop.description && (
															<div className="prop-name-desc">{prop.description}</div>
														)}
													</td>
													{showDescriptions && <td className="prop-description">{prop.description}</td>}
													<td className={valueClass}>
														<div className="prop-value-row">
															<span className="prop-value-text">{display}</span>
															{tag === 'default' && <span className="prop-value-tag">default</span>}
															{tag === 'pending' && <span className="prop-value-tag prop-value-tag-pending">pending</span>}
															{prop.isEditable && (
																<button
																	className="prop-edit-btn"
																	onClick={() => openEdit(prop)}
																	title={`Edit ${prop.name}`}
																	aria-label={`Edit ${prop.name}`}
																>
																	✎
																</button>
															)}
														</div>
														{pending && (
															<div className="prop-value-was">
																was: <code>{pending.baseSource === 'unset' ? '(unset)' : pending.baseValue}</code>
																{pending.baseSource === 'default' && ' (default)'}
															</div>
														)}
													</td>
												</tr>
											)
										})}
									</tbody>
								</table>
							)}
						</section>
					)
				})}

			{describing && <DescriptionModal prop={describing} onClose={() => setDescribing(null)} />}

			{editing && (
				<EditPropertyModal
					prop={editing.prop}
					initialValue={editing.initialValue}
					initialSource={editing.initialSource}
					currentValue={editing.currentValue}
					isStaged={editing.isStaged}
					validatePath={adapter.validatePath}
					onConfirm={confirmEdit}
					onUnstage={() => {
						removeFromBasket(editing.prop.name)
						setEditing(null)
					}}
					onCancel={() => setEditing(null)}
				/>
			)}

			{showBasket && (
				<BasketModal
					basket={basket}
					propsByName={propsByName}
					values={values}
					applying={applying}
					applyError={applyError}
					conflicts={conflicts}
					onRemove={removeFromBasket}
					onKeepPending={keepPending}
					onAdoptCurrent={adoptCurrent}
					onClose={() => {
						setShowBasket(false)
						setConflicts([])
						setApplyError(null)
					}}
					onApply={applyBasket}
				/>
			)}

			{blocker.state === 'blocked' && (
				<ConfirmLeaveModal
					count={basketCount}
					onDiscard={() => blocker.proceed()}
					onStay={() => blocker.reset()}
				/>
			)}
		</div>
	)
}
