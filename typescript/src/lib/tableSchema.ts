// Types and helpers for building a Sleeper table schema in the create-table wizard.
// The schema JSON shape here matches Sleeper's SchemaSerDe (row/sort/value fields, type
// serialisation, `nullable` only emitted when true). Structural validation is NOT done
// here — the wizard posts drafts to /api/sleeper/schema/validate

export type PrimitiveTypeName = 'IntType' | 'LongType' | 'StringType' | 'ByteArrayType'

export const PRIMITIVE_TYPES: PrimitiveTypeName[] = ['IntType', 'LongType', 'StringType', 'ByteArrayType']

export const PRIMITIVE_TYPE_LABELS: Record<PrimitiveTypeName, string> = {
	IntType: 'Int',
	LongType: 'Long',
	StringType: 'String',
	ByteArrayType: 'Byte array',
}

export type ValueTypeKind = PrimitiveTypeName | 'MapType' | 'ListType'

export interface KeyFieldDraft {
	name: string
	type: PrimitiveTypeName
}

export interface ValueFieldDraft {
	name: string
	kind: ValueTypeKind
	// Only relevant when kind === 'MapType'
	mapKeyType: PrimitiveTypeName
	mapValueType: PrimitiveTypeName
	// Only relevant when kind === 'ListType'
	elementType: PrimitiveTypeName
	nullable: boolean
}

export interface SchemaDraft {
	rowKeys: KeyFieldDraft[]
	sortKeys: KeyFieldDraft[]
	valueFields: ValueFieldDraft[]
}

// ── Serialisation to the JSON shape Sleeper's SchemaSerDe expects ──

type SchemaTypeJson = PrimitiveTypeName
	| { MapType: { keyType: PrimitiveTypeName; valueType: PrimitiveTypeName } }
	| { ListType: { elementType: PrimitiveTypeName } }

interface SchemaFieldJson {
	name: string
	type: SchemaTypeJson
	nullable?: boolean
}

interface SchemaJson {
	rowKeyFields: SchemaFieldJson[]
	sortKeyFields: SchemaFieldJson[]
	valueFields: SchemaFieldJson[]
}

function keyFieldToJson(field: KeyFieldDraft): SchemaFieldJson {
	return { name: field.name, type: field.type }
}

function valueFieldToJson(field: ValueFieldDraft): SchemaFieldJson {
	let type: SchemaTypeJson
	if (field.kind === 'MapType') {
		type = { MapType: { keyType: field.mapKeyType, valueType: field.mapValueType } }
	} else if (field.kind === 'ListType') {
		type = { ListType: { elementType: field.elementType } }
	} else {
		type = field.kind
	}
	const json: SchemaFieldJson = { name: field.name, type }
	if (field.nullable) json.nullable = true
	return json
}

export function schemaDraftToJson(draft: SchemaDraft): SchemaJson {
	return {
		rowKeyFields: draft.rowKeys.map(keyFieldToJson),
		sortKeyFields: draft.sortKeys.map(keyFieldToJson),
		valueFields: draft.valueFields.map(valueFieldToJson),
	}
}

export function schemaDraftToJsonString(draft: SchemaDraft, pretty = false): string {
	return JSON.stringify(schemaDraftToJson(draft), null, pretty ? 2 : undefined)
}

export function firstRowKeyType(draft: SchemaDraft): PrimitiveTypeName | null {
	return draft.rowKeys.length > 0 ? draft.rowKeys[0].type : null
}

// ── Parse an existing schema JSON (e.g. copied from another table) into a draft ──

function isPrimitiveTypeName(value: unknown): value is PrimitiveTypeName {
	return typeof value === 'string' && (PRIMITIVE_TYPES as string[]).includes(value)
}

function jsonToKeyField(field: SchemaFieldJson): KeyFieldDraft {
	if (!isPrimitiveTypeName(field.type)) {
		throw new Error(`Field "${field.name}" has a non-primitive key type`)
	}
	return { name: field.name, type: field.type }
}

function jsonToValueField(field: SchemaFieldJson): ValueFieldDraft {
	const draft = newValueField()
	draft.name = field.name
	draft.nullable = field.nullable === true
	const type = field.type
	if (isPrimitiveTypeName(type)) {
		draft.kind = type
	} else if (type && typeof type === 'object' && 'MapType' in type) {
		draft.kind = 'MapType'
		draft.mapKeyType = type.MapType.keyType
		draft.mapValueType = type.MapType.valueType
	} else if (type && typeof type === 'object' && 'ListType' in type) {
		draft.kind = 'ListType'
		draft.elementType = type.ListType.elementType
	} else {
		throw new Error(`Field "${field.name}" has an unrecognised type`)
	}
	return draft
}

export function parseSchemaString(schemaJson: string): SchemaDraft {
	const parsed = JSON.parse(schemaJson) as SchemaJson
	return {
		rowKeys: (parsed.rowKeyFields ?? []).map(jsonToKeyField),
		sortKeys: (parsed.sortKeyFields ?? []).map(jsonToKeyField),
		valueFields: (parsed.valueFields ?? []).map(jsonToValueField),
	}
}

// ── Factory helpers for new rows ──

export function newKeyField(): KeyFieldDraft {
	return { name: '', type: 'StringType' }
}

export function newValueField(): ValueFieldDraft {
	return {
		name: '',
		kind: 'StringType',
		mapKeyType: 'StringType',
		mapValueType: 'StringType',
		elementType: 'StringType',
		nullable: false,
	}
}

// ── Schema templates (used by the picker on the Schema step) ──

export interface SchemaTemplate {
	id: string
	label: string
	description: string
	schema: SchemaDraft
}

function key(name: string, type: PrimitiveTypeName): KeyFieldDraft {
	return { name, type }
}

function primitiveValue(name: string, kind: PrimitiveTypeName, nullable = false): ValueFieldDraft {
	return { ...newValueField(), name, kind, nullable }
}

function mapValue(name: string, mapKeyType: PrimitiveTypeName, mapValueType: PrimitiveTypeName, nullable = true): ValueFieldDraft {
	return { ...newValueField(), name, kind: 'MapType', mapKeyType, mapValueType, nullable }
}

export const SCHEMA_TEMPLATES: SchemaTemplate[] = [
	{
		id: 'key-value',
		label: 'Simple key-value',
		description: 'A string key with a single string value.',
		schema: {
			rowKeys: [key('key', 'StringType')],
			sortKeys: [],
			valueFields: [primitiveValue('value', 'StringType')],
		},
	},
	{
		id: 'versioned-key-value',
		label: 'Versioned key-value',
		description: 'A string key, sorted by time, with a single string value.',
		schema: {
			rowKeys: [key('key', 'StringType')],
			sortKeys: [key('timestamp', 'LongType')],
			valueFields: [primitiveValue('value', 'StringType')],
		},
	},
	{
		id: 'time-series',
		label: 'Time-series',
		description: 'Readings keyed by timestamp, sorted by sensor, with an optional metadata map.',
		schema: {
			rowKeys: [key('timestamp', 'LongType')],
			sortKeys: [key('sensorId', 'StringType')],
			valueFields: [primitiveValue('reading', 'LongType'), mapValue('metadata', 'StringType', 'StringType')],
		},
	},
	{
		id: 'user-events',
		label: 'User events',
		description: 'Events keyed by user, sorted by event time, with an attributes map.',
		schema: {
			rowKeys: [key('userId', 'StringType')],
			sortKeys: [key('eventTime', 'LongType')],
			valueFields: [primitiveValue('eventType', 'StringType'), mapValue('attributes', 'StringType', 'StringType')],
		},
	}
]

// ── Split-point examples (used by the picker on the Split points step) ──
// Keyed by first-row-key type; the picker shows only examples matching the current type.

export interface SplitPointExample {
	id: string
	label: string
	rowKeyType: PrimitiveTypeName
	// Static split points, or a generator invoked only when the example is selected
	lines?: string[]
	generate?: () => string[]
}

const STRING_SPLIT_POINT_PARTITION_COUNTS = [8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]

// Produce `partitions - 1` ascending lowercase strings that split the [a…z] key space into
// evenly sized ranges. Each split point is a fixed-width base-26 encoding of the boundary
// fraction i / partitions, so the values are uniformly spaced across the alphabet.
export function generateStringSplitPoints(partitions: number, width = 10): string[] {
	const RADIX = 26n
	// Scale factor as a bigint so we can place boundaries precisely without float drift.
	const scale = RADIX ** BigInt(width)
	const lines: string[] = []
	for (let i = 1; i < partitions; i++) {
		let value = (scale * BigInt(i)) / BigInt(partitions)
		let s = ''
		for (let d = 0; d < width; d++) {
			const digit = Number(value % RADIX)
			s = String.fromCharCode(97 + digit) + s
			value /= RADIX
		}
		lines.push(s)
	}
	return lines
}

export const SPLIT_POINT_EXAMPLES: SplitPointExample[] = [
	...STRING_SPLIT_POINT_PARTITION_COUNTS.map((n) => ({
		id: `string-${n}`,
		label: `Alphabetical, even distribution (${n} partitions)`,
		rowKeyType: 'StringType' as const,
		generate: () => generateStringSplitPoints(n),
	})),
	{
		id: 'long-days',
		label: 'Daily boundaries (Unix ms)',
		rowKeyType: 'LongType',
		// 2024-01-01, -02, -03, -04 at 00:00 UTC
		lines: ['1704067200000', '1704153600000', '1704240000000', '1704326400000'],
	},
	{
		id: 'int-ranges',
		label: 'Even ranges (200, 400, 600, 800)',
		rowKeyType: 'IntType',
		lines: ['200', '400', '600', '800'],
	},
	{
		id: 'bytes-base64',
		label: 'Base64 boundaries',
		rowKeyType: 'ByteArrayType',
		// base64 of "M" and "f"
		lines: ['TQ==', 'Zg=='],
	},
]

// Resolve an example's split points, running its generator if it has one.
export function splitPointExampleLines(example: SplitPointExample): string[] {
	return example.generate ? example.generate() : example.lines ?? []
}

export function splitPointExamplesForType(type: PrimitiveTypeName | null): SplitPointExample[] {
	if (!type) return []
	return SPLIT_POINT_EXAMPLES.filter((e) => e.rowKeyType === type)
}
