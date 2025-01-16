import { startOfToday } from 'date-fns'
import { pool, queryLatestDailyMd } from '../src/db'
import { fetchAllAdjFactor, ts } from '../src/tasks'
import { dateToTsDate, type TsDate } from '../src/tushare'

await fetchAllAdjFactor()

await pool.end()
