import cliProgress from 'cli-progress'
import { startOfToday } from 'date-fns'
import _ from 'lodash'
import { queryAllStockBasic, queryLatestDailyMd, upsertAdjFactor, upsertDailyMd, upsertStockBasic } from './db'
import tushare, { AllStockBasicFields, dateToTsDate, type TsDate } from './tushare'

if (!process.env.TOKEN) {
  throw new Error('TOKEN environment variable is required')
}

export const ts = new tushare(process.env.TOKEN)

export const fetchAllStockBasic = async () => {
  const listed = await ts.fetchStockBasic({
    list_status: 'L',
  }, AllStockBasicFields)

  await upsertStockBasic(listed)

  const delisted = await ts.fetchStockBasic({
    list_status: 'D',
  }, AllStockBasicFields)

  await upsertStockBasic(delisted)
}

export const fetchDailyMd = async () => {
  const daily = await ts.fetchDailyMd({
    trade_date: dateToTsDate(new Date()),
  })
  await upsertDailyMd(daily)
}

export const fetchMissingDailyMd = async () => {
  const latestDate = await queryLatestDailyMd()
  if (!latestDate) {
    // 如果数据库中没有记录，则直接更新所有数据
    await fetchAllDailyMd()
    return
  }

  const dates = ((await ts.fetchTradeCal({
    start_date: latestDate,
    end_date: dateToTsDate(startOfToday()),
  })).data.items.map((item) => item[1] as TsDate)).sort()

  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
  bar.start(dates.length, 0)

  await Promise.all(dates.map(async (date) => {
    const daily = await ts.fetchDailyMd({
      trade_date: date,
    })
    await upsertDailyMd(daily)
    bar.increment(1)
  }))

  bar.stop()
}

export const fetchAllDailyMd = async () => {
  const items = await queryAllStockBasic()

  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
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

export const fetchDailyAdjFactor = async () => {
  const adjFactor = await ts.fetchAdjFactor({
    trade_date: dateToTsDate(new Date()),
  })
  await upsertAdjFactor(adjFactor)
}

export const fetchAllAdjFactor = async () => {
  const items = await queryAllStockBasic()

  const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic)
  bar.start(items.length, 0)

  await Promise.all(items.map(async (item) => {
    const adjFactor = await ts.fetchAdjFactor({ ts_code: item.ts_code })
    await upsertAdjFactor(adjFactor)
    bar.increment(1)
  }))
  bar.stop()
}
