import { pool } from './db'
import { dailyMd, stockBasic } from './tasks'

await stockBasic()

await dailyMd()

await pool.end()
