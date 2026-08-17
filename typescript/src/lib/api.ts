
const API_PREFIX = '/api'

export function apiUrl(path: string): string {
	return API_PREFIX + path
}

export function apiFetch(path: string, init?: RequestInit): Promise<Response> {
	return fetch(apiUrl(path), init)
}

export function postJson(path: string, body: unknown, init?: RequestInit): Promise<Response> {
	return apiFetch(path, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify(body),
		...init,
	})
}

export function postRaw(path: string, body: string, init?: RequestInit): Promise<Response> {
	return apiFetch(path, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body,
		...init,
	})
}