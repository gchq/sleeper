import { useInstance } from "../contexts/InstanceContext"

type TitleChild = string | number

interface TitleProps {
	children: TitleChild | TitleChild[]
}

export default function Title({ children }: TitleProps) {
	const { instanceId } = useInstance()
	const title = (Array.isArray(children) ? children.join('') : String(children)) + ' - ' + instanceId + ' - Sleeper'
	return <title>{title}</title>
}
