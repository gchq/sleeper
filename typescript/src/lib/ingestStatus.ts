
export function statusLabel(status: string): string {
	return status
		.toLowerCase()
		.split('_')
		.map(word => word.charAt(0).toUpperCase() + word.slice(1))
		.join(' ')
}

export function statusClass(status: string): string {
	switch (status.toUpperCase()) {
		case 'FINISHED':
			return 'jobs-status finished'
		case 'FAILED':
		case 'REJECTED':
			return 'jobs-status failed'
		case 'IN_PROGRESS':
		case 'UNCOMMITTED':
			return 'jobs-status in-progress'
		default:
			return 'jobs-status pending'
	}
}
