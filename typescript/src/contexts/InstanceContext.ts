import { createContext, useContext } from 'react'

export interface TableStatus {
	tableUniqueId: string
	tableName: string
	online: boolean
}

export interface InstanceFeatures {
	IngestBatcher?: boolean
}

export interface InstanceInfo {
	instanceId: string
	version: string
	tables: TableStatus[]
	features: InstanceFeatures
}

export interface InstanceValue {
	instanceId: string | null
	version: string | null
	tables: TableStatus[] | null
	features: InstanceFeatures | null
	loading: boolean
	error: string | null
	reload: () => void
}

export const InstanceContext = createContext<InstanceValue | null>(null)

export function useInstance(): InstanceValue {
	const ctx = useContext(InstanceContext)
	if (!ctx) throw new Error('useInstance must be used within an InstanceProvider')
	return ctx
}
