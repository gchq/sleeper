import { createContext, useContext, useMemo, type ReactNode } from 'react'
import { useApi } from '../hooks/useApi'

export interface TableStatus {
	tableUniqueId: string
	tableName: string
	online: boolean
}

interface TablesListValue {
	tables: TableStatus[] | null
	loading: boolean
	error: string | null
}

const TablesContext = createContext<TablesListValue | null>(null)

export function TablesProvider({ children }: { children: ReactNode }) {
	const { data: tables, loading, error } = useApi<TableStatus[]>('/tables')
	const value = useMemo<TablesListValue>(() => ({ tables, loading, error }), [tables, loading, error])
	return <TablesContext.Provider value={value}>{children}</TablesContext.Provider>
}

export function useTablesList(): TablesListValue {
	const ctx = useContext(TablesContext)
	if (!ctx) throw new Error('useTablesList must be used within a TablesProvider')
	return ctx
}
