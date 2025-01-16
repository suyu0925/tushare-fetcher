import { pool } from '../src/db'
import { fetchAllAdjFactor, fetchAllDailyMd, fetchAllStockBasic } from '../src/tasks'

await fetchAllStockBasic()

await fetchAllDailyMd()

await fetchAllAdjFactor()

await pool.end()
