import { pool } from '../src/db'
import { fetchAllStockBasic, fetchDailyAdjFactor, fetchDailyMd } from '../src/tasks'

await fetchAllStockBasic()

await fetchDailyMd()

await fetchDailyAdjFactor()

await pool.end()
