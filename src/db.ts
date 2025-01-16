import { addDays } from 'date-fns'
import _ from 'lodash'
import pg from 'pg'
import { dateToTsDate, type GetDailyResponse, type GetStockBasicItem, type GetStockBasicResponse, type TsDate } from './tushare'

if (!process.env.PGURL) {
  throw new Error('PGURL environment variable is required')
}

export const pool = new pg.Pool({
  connectionString: process.env.PGURL,
})

export const queryAllListedStockBasic = async () => {
  const res = await pool.query<GetStockBasicItem>(`SELECT * FROM stock_basic WHERE list_status = 'L'`)
  return res.rows
}

export const queryAllStockBasic = async () => {
  const res = await pool.query<GetStockBasicItem>(`SELECT * FROM stock_basic`)
  return res.rows
}

/**
 * 查询日度行情更新到哪一天
 * 注意不会考虑停牌股票，所以返回的日期可能会有较大误差，不要用于每日更新
 */
export const queryLatestDailyMd = async (): Promise<TsDate | null> => {
  const stocks = await queryAllListedStockBasic()
  const latest_records = await pool.query<{
    ts_code: string
    last_trade_date: TsDate
  }>(`
    SELECT ts_code, MAX(trade_date) AS last_trade_date 
    FROM daily_md 
    GROUP BY ts_code
  `)
  const latestDates = stocks.map(stock => {
    const record = latest_records.rows.find(record => record.ts_code === stock.ts_code)
    if (!record) {
      // 如果数据库中没有该股票的记录，则返回其上市日期
      return stock.list_date
    }
    if (stock.delist_date && record.last_trade_date === stock.delist_date) {
      // 如果该股票已退市，且上市期间的数据已全部更新，返回一个未来时间
      return dateToTsDate(addDays(new Date(), 1))
    }
    return record.last_trade_date
  })
  return _.min(latestDates) ?? null
}

export const upsertStockBasic = async (res: GetStockBasicResponse) => {
  const { data } = res

  const values = data.items
  if (values.length === 0) {
    return
  }

  await Promise.all(_.chunk(values, 1000)
    .map(async (chunk) => {
      // 为每行数据创建参数占位符
      const valueParams = chunk.map((_, rowIndex) =>
        `(${data.fields.map((_, colIndex) => `$${rowIndex * data.fields.length + colIndex + 1}`).join(', ')})`
      ).join(',\n    ')

      const query = `
      INSERT INTO stock_basic (${data.fields.join(', ')}) 
      VALUES ${valueParams}
      ON CONFLICT (symbol) 
      DO UPDATE SET ${data.fields
          .filter(field => field !== 'symbol') // 排除主键
          .map(field => `${field} = EXCLUDED.${field}`)
          .join(', ')}`
      await pool.query(query, chunk.flat())
    }))
}

export const upsertDailyMd = async (res: GetDailyResponse) => {
  const { data } = res
  const values = data.items
  if (values.length === 0) {
    return
  }

  // 使用事务，保证当日行情数据的插入一致性
  const client = await pool.connect()
  try {
    await client.query('BEGIN')
    await Promise.all(_.chunk(values, 1000)
      .map(async (chunk) => {
        // 为每行数据创建参数占位符
        const valueParams = chunk.map((_, rowIndex) =>
          `(${data.fields.map((_, colIndex) => `$${rowIndex * data.fields.length + colIndex + 1}`).join(', ')})`
        ).join(',\n    ')

        const query = `
      INSERT INTO daily_md (${data.fields.join(', ')}) 
      VALUES ${valueParams}
      ON CONFLICT (ts_code, trade_date) 
      DO UPDATE SET ${data.fields
            .filter(field => field !== 'ts_code' && field !== 'trade_date') // 排除主键
            .map(field => `${field} = EXCLUDED.${field}`)
            .join(', ')}`
        await client.query(query, chunk.flat())
      }))
    await client.query('COMMIT')
  } catch (e) {
    await client.query('ROLLBACK')
    throw e
  } finally {
    client.release()
  }
}

export const initDb = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS stock_basic (
      ts_code text NOT NULL,
      symbol text PRIMARY KEY,
      name text NOT NULL,
      area text,
      industry text,
      fullname text,
      enname text,
      cnspell text NOT NULL,
      market text,
      exchange text,
      curr_type text,
      list_status text,
      list_date text NOT NULL,
      delist_date text,
      is_hs text,
      act_name text,
      act_ent_type text
    );
    CREATE INDEX IF NOT EXISTS idx_ts_code ON stock_basic (ts_code);
    CREATE INDEX IF NOT EXISTS idx_list_status ON stock_basic (list_status);
    CREATE INDEX IF NOT EXISTS idx_list_date ON stock_basic (list_date);
    CREATE INDEX IF NOT EXISTS idx_delist_date ON stock_basic (delist_date);
  `)

  await pool.query(`
    CREATE TABLE IF NOT EXISTS daily_md (
      ts_code text NOT NULL,
      trade_date text NOT NULL,
      open numeric NOT NULL,
      high numeric NOT NULL,
      low numeric NOT NULL,
      close numeric NOT NULL,
      pre_close numeric NOT NULL,
      change numeric NOT NULL,
      pct_chg numeric NOT NULL,
      vol numeric NOT NULL,
      amount numeric NOT NULL,
      PRIMARY KEY (ts_code, trade_date)
    );
    CREATE INDEX IF NOT EXISTS idx_ts_code ON daily_md (ts_code);
    CREATE INDEX IF NOT EXISTS idx_trade_date ON daily_md (trade_date);
  `)
}

pool.on('connect', (client) => {
  const schema = new URL(process.env.PGURL!).searchParams.get('schema') || 'public'
  client.query(`SET search_path TO ${schema}`)
})

await initDb()
