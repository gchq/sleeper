import { createContext, useContext } from 'react'

export interface TableStatus {
	tableUniqueId: string
	tableName: string
	online: boolean
}

export interface TablesListValue {
	tables: TableStatus[] | null
	loading: boolean
	error: string | null
	reload: () => void
}

export const TablesContext = createContext<TablesListValue | null>(null)

export function useTablesList(): TablesListValue {
	const ctx = useContext(TablesContext)
	if (!ctx) throw new Error('useTablesList must be used within a TablesProvider')
	return ctx
}
