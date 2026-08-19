import { useParams } from 'react-router-dom'
import type { InstanceValue, TableStatus } from '../contexts/InstanceContext'
import { useInstance } from '../contexts/InstanceContext'

// table is undefined if the current URL does not contain a tableId param
// table is null if the current URL contains a tableId param but that tableId does not exist
export interface SelectedTable extends InstanceValue {
	table?: TableStatus | null
}

export function useSelectedTable(): SelectedTable {
	const { tableId } = useParams<{ tableId?: string }>()
	const instance = useInstance()
	if (!tableId) return instance
	return { ...instance, table: instance.tables?.find((t) => t.tableUniqueId === tableId) ?? null }
}
