import cliProgress from 'cli-progress'
import _ from 'lodash'
import { queryAllListedStockBasic, queryAllStockBasic, upsertDailyMd, upsertStockBasic } from './db'
import tushare, { AllStockBasicFields } from './tushare'

if (!process.env.TOKEN) {
  throw new Error('TOKEN environment variable is required')
}

export const ts = new tushare(process.env.TOKEN)

export const stockBasic = async () => {
  const listed = await ts.fetchStockBasic({
    list_status: 'L',
  }, AllStockBasicFields)

  await upsertStockBasic(listed)

  const delisted = await ts.fetchStockBasic({
    list_status: 'D',
  }, AllStockBasicFields)

  await upsertStockBasic(delisted)
}

export const dailyMd = async () => {
  const items = await queryAllListedStockBasic()
  const daily = await ts.fetchDailyMd({
    ts_code: items.map(item => item.ts_code).join(','),
  })
  await upsertDailyMd(daily)
}

export const fetchAllDailyMd = async () => {
  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)

  const items = await queryAllStockBasic()
  bar.start(items.length, 0)

  const chunkLength = 2
  for (const chunk of _.chunk(items, chunkLength)) {
    await Promise.all(chunk.map(async (item) => {
      const daily = await ts.fetchDailyMd({
        ts_code: item.ts_code,
        start_date: item.list_date,
        end_date: item.delist_date,
      })
      await upsertDailyMd(daily)
    }))
    bar.increment(chunkLength)
  }

  bar.stop()
}
