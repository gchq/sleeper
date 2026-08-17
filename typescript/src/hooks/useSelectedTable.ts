import { useParams } from 'react-router-dom'
import type { TableStatus } from '../contexts/InstanceContext'
import { useInstance } from '../contexts/InstanceContext'

export interface SelectedTable {
	table: TableStatus | null
	loading: boolean
	error: string | null
}

export function useSelectedTable(): SelectedTable {
	const { tableId } = useParams<{ tableId?: string }>()
	const { tables, loading, error } = useInstance()
	if (!tableId || !tables) return { table: null, loading, error }
	return { table: tables.find((t) => t.tableUniqueId === tableId) ?? null, loading, error }
}
