import { createContext, useContext } from 'react'
import type { FeatureName } from '../lib/features'

export interface TableStatus {
	tableUniqueId: string
	tableName: string
	online: boolean
}

export type InstanceFeatures = Partial<Record<FeatureName, boolean>>

export interface InstanceInfo {
	instanceId: string
	version: string
	region: string
	tables: TableStatus[]
	features: InstanceFeatures
}

export interface InstanceValue {
	instanceId: string | null
	version: string | null
	region: string | null
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
