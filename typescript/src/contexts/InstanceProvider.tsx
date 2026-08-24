import { useMemo, type ReactNode } from 'react'
import { useApi } from '../hooks/useApi'
import { InstanceContext, type InstanceInfo, type InstanceValue } from './InstanceContext'

export function InstanceProvider({ children }: { children: ReactNode }) {
	const { data, loading, error, reload } = useApi<InstanceInfo>('/instance')
	const value = useMemo<InstanceValue>(
		() => ({
			instanceId: null,
			version: null,
			region: null,
			tables: null,
			features: null,
			...data,
			loading,
			error,
			reload,
		}),
		[data, loading, error, reload],
	)
	return <InstanceContext.Provider value={value}>{children}</InstanceContext.Provider>
}
