import { pool } from './db'
import { fetchAllDailyMd } from './tasks'

await fetchAllDailyMd()

await pool.end()
