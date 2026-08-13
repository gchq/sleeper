import PropertiesPage from '../components/PropertiesPage'
import Title from '../components/Title'

export default function InstanceProperties() {
	return (
		<>
			<Title>Instance Properties</Title>

			<PropertiesPage
				adapter={{
					title: 'Instance Properties',
					definitionsPath: '/sleeper/instance/properties',
					valuesPath: '/instance/properties',
					validatePath: '/sleeper/instance/properties/validate',
					savePath: '/instance/properties',
				}}
			/>
		</>
	)
}
