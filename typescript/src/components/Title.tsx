import { useInstance } from "../contexts/InstanceContext"

type TitleChild = string | number

interface TitleProps {
	children?: TitleChild | TitleChild[]
}

export default function Title({ children }: TitleProps) {
	const { instanceId } = useInstance()
	const page = children === undefined ? '' : Array.isArray(children) ? children.join('') : String(children)
	const parts: string[] = []
	if (page) parts.push(page)
	if (instanceId) parts.push(instanceId)
	parts.push('Sleeper')
	return <title>{parts.join(' - ')}</title>
}
