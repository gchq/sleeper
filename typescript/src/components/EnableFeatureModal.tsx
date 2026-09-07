import { useEffect } from 'react'
import { Link } from 'react-router-dom'
import { enableInstruction, type FeatureInfo } from '../lib/features'
import './EnableFeatureModal.css'

interface Props {
	feature: FeatureInfo
	onClose: () => void
}

export default function EnableFeatureModal({ feature, onClose }: Props) {
	useEffect(() => {
		function onKey(e: KeyboardEvent) {
			if (e.key === 'Escape') onClose()
		}
		window.addEventListener('keydown', onKey)
		return () => window.removeEventListener('keydown', onKey)
	}, [onClose])

	const { kind, property, value, link } = enableInstruction(feature.key)
	const Icon = feature.icon

	return (
		<div className="modal-backdrop" onClick={onClose}>
			<div
				className="modal enable-modal"
				onClick={(e) => e.stopPropagation()}
				role="dialog"
				aria-modal="true"
				aria-label={'How to enable ' + feature.name}
			>
				<div className="enable-modal-head">
					<span className="enable-modal-icon" aria-hidden="true">
						<Icon width={40} height={40} />
					</span>
					<div>
						<h3 className="enable-modal-title">{feature.name}</h3>
						<span className="enable-modal-status">Not deployed</span>
					</div>
				</div>

				<p className="modal-description">{feature.description}</p>

				<div className="enable-modal-steps">
					<div className="enable-modal-steps-label">How to enable</div>
					<ol>
						<li>
							{kind === 'stack' ? (
								<>
									Add <code>{value}</code> to the comma-separated <code>{property}</code> instance property,
									keeping the stacks already listed there.
								</>
							) : (
								<>
									Set the <code>{property}</code> instance property to <code>{value}</code>.
								</>
							)}
						</li>
						<li>Redeploy the instance for the change to take effect.</li>
					</ol>
				</div>

				<div className="modal-actions">
					<Link to={link} className="btn btn-primary" onClick={onClose}>
						Go to {property}
					</Link>
					<button type="button" className="btn" onClick={onClose}>
						Close
					</button>
				</div>
			</div>
		</div>
	)
}
