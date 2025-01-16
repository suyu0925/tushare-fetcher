import { startOfToday } from 'date-fns'
import { pool, queryLatestDailyMd } from '../src/db'
import { ts } from '../src/tasks'
import { dateToTsDate, type TsDate } from '../src/tushare'

const latestDate = await queryLatestDailyMd()
if (!latestDate) {
  console.log('No latest date found, exiting...')
  process.exit(0)
}

const dates = ((await ts.fetchTradeCal({
  start_date: latestDate,
  end_date: dateToTsDate(startOfToday()),
})).data.items.map((item) => item[1] as TsDate)).sort()

console.log(dates)

await pool.end()
