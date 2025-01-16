import { pool } from '../src/db'
import { fetchAllStockBasic, fetchDailyMd } from '../src/tasks'

await fetchAllStockBasic()

await fetchDailyMd()

await pool.end()
