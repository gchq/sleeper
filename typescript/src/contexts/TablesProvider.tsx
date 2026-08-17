import { useMemo, type ReactNode } from 'react'
import { useApi } from '../hooks/useApi'
import { TablesContext, type TableStatus, type TablesListValue } from './TablesContext'

export function TablesProvider({ children }: { children: ReactNode }) {
	const { data: tables, loading, error, reload } = useApi<TableStatus[]>('/tables')
	const value = useMemo<TablesListValue>(
		() => ({ tables, loading, error, reload }),
		[tables, loading, error, reload],
	)
	return <TablesContext.Provider value={value}>{children}</TablesContext.Provider>
}
