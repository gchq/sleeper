
const LAST_TABLE_STORAGE_KEY = 'sleeper-sidebar-last-table'

export function readLastTable(): string | null {
	try {
		return window.localStorage.getItem(LAST_TABLE_STORAGE_KEY)
	} catch {
		return null
	}
}

export function writeLastTable(tableId: string) {
	try {
		window.localStorage.setItem(LAST_TABLE_STORAGE_KEY, tableId)
	} catch {
		// Ignore localStorage failures (e.g. private mode or quota exceeded)
	}
}

export function clearLastTable() {
	try {
		window.localStorage.removeItem(LAST_TABLE_STORAGE_KEY)
	} catch {
		// Ignore localStorage failures (e.g. private mode or quota exceeded)
	}
}