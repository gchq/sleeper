type TitleChild = string | number

interface TitleProps {
	children: TitleChild | TitleChild[]
}

export default function Title({ children }: TitleProps) {
	const title = (Array.isArray(children) ? children.join('') : String(children)) + ' - Sleeper'
	return <title>{title}</title>
}
