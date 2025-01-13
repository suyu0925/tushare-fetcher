import pg from 'pg'
import type { GetDailyResponse, GetStockBasicFields, GetStockBasicItem, GetStockBasicResponse } from './tushare'
import _ from 'lodash'

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
      await pool.query(query, chunk.flat())
    }))
}

export const initDb = async () => {
  await pool.query(`CREATE TABLE IF NOT EXISTS stock_basic (
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
  )`)

  await pool.query(`CREATE TABLE IF NOT EXISTS daily_md (
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
  )`)
}

pool.on('connect', (client) => {
  const schema = new URL(process.env.PGURL!).searchParams.get('schema') || 'public'
  client.query(`SET search_path TO ${schema}`)
})

await initDb()
