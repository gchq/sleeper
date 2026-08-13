import { useParams } from 'react-router-dom'
import type { TableStatus } from '../contexts/TablesContext'
import { useTablesList } from '../contexts/TablesContext'

export interface SelectedTable {
	table: TableStatus | null
	loading: boolean
	error: string | null
}

export function useSelectedTable(): SelectedTable {
	const { tableId } = useParams<{ tableId?: string }>()
	const { tables, loading, error } = useTablesList()
	if (!tableId || !tables) return { table: null, loading, error }
	return { table: tables.find((t) => t.tableUniqueId === tableId) ?? null, loading, error }
}
