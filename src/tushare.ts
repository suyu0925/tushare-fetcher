import { format, parse } from 'date-fns'
import type { Tagged } from 'type-fest'
import { RateLimiter } from './rateLimiter'

type ApiResponse<T> = {
  request_id: string
  code: number
  data: T
  msg: string
}

export type TsDate = Tagged<string, 'TsDate'> // 日期（YYYYMMDD）

export const tsDateToDate = (tsDate: TsDate): Date => parse(tsDate, 'yyyyMMdd', new Date())
export const dateToTsDate = (date: Date): TsDate => format(date, 'yyyyMMdd') as TsDate

type ListStatus = 'L' | 'D' | 'P'  // 上市状态 L上市 D退市 P暂停上市

type ExchangeCode = 'SSE' | 'SZSE' | 'BSE' // 交易所 SSE上交所 SZSE深交所 BSE北交所

type GetStockBasicParams = {
  ts_code?: string // TS股票代码
  name?: string // 股票名称
  market?: string // 市场类别 （主板/创业板/科创板/CDR/北交所）
  list_status?: ListStatus // 上市状态，默认是L
  exchange?: ExchangeCode // 交易所
  is_hs?: 'N' | 'H' | 'S' // 是否沪深港通标的，N否，H沪股通，S深股通
  limit?: number // 返回数量，默认是10000
  offset?: number // 偏移量，默认是0
}

export type GetStockBasicItem = {
  ts_code: string // TS股票代码
  symbol: string // 股票代码
  name: string // 股票名称
  area: string | null // 所在地域，北交所的股票area为null
  industry: string | null // 所属行业，北交所的股票industry为null
  fullname?: string // 股票全称
  enname?: string // 英文全称
  cnspell: string // 拼音缩写
  market?: string // 市场类别 （主板/创业板/科创板/CDR/北交所），退市的股票market为null
  exchange?: ExchangeCode // 交易所代码
  curr_type?: string // 交易货币
  list_status?: ListStatus // 上市状态
  list_date: TsDate // 上市日期
  delist_date?: TsDate // 退市日期
  is_hs?: string // 是否沪深港通标的
  act_name: string | null // 实控人名称
  act_ent_type: string | null // 实控人企业性质
}

export type GetStockBasicFields = keyof GetStockBasicItem

export const AllStockBasicFields: GetStockBasicFields[] = [
  'ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname', 'cnspell',
  'market', 'exchange', 'curr_type', 'list_status', 'list_date', 'delist_date',
  'is_hs', 'act_name', 'act_ent_type',
]

export type GetStockBasicResponse = ApiResponse<{
  fields: GetStockBasicFields[]
  items: GetStockBasicItem[GetStockBasicFields][][]
  has_more: boolean
  count: -1 // 这个字段在文档中没有描述，但是返回的实际数据中是-1
}>

/**
 * A股日线行情
 * 数据说明：交易日每天15点～16点之间入库。本接口是未复权行情，停牌期间不提供数据
 * @see https://tushare.pro/document/2?doc_id=27
 */
type GetDailyParams = {
  ts_code: string // 股票代码（支持多个股票同时提取，逗号分隔）
  start_date?: TsDate // 开始日期
  end_date?: TsDate // 结束日期
} | {
  trade_date: TsDate
}

export type GetDailyItem = {
  ts_code: string // 股票代码
  trade_date: TsDate // 交易日期
  open: number // 开盘价
  high: number // 最高价
  low: number // 最低价
  close: number // 收盘价
  pre_close: number // 昨收价【除权价，前复权】
  change: number // 涨跌额
  pct_chg: number // 涨跌幅 【基于除权后的昨收计算的涨跌幅：（今收-除权昨收）/除权昨收 】
  vol: number // 成交量（手）
  amount: number // 成交额（千元）
}

type GetDailyFields = keyof GetDailyItem

export const AllDailyFields: GetDailyFields[] = [
  'ts_code', 'trade_date',
  'open', 'high', 'low', 'close', 'pre_close',
  'change', 'pct_chg', 'vol', 'amount',
]

export type GetDailyResponse = ApiResponse<{
  fields: GetDailyFields[]
  items: GetDailyItem[GetDailyFields][][]
  has_more: boolean
  count: -1 // 这个字段在文档中没有描述，但是返回的实际数据中是-1
}>

/**
 * 交易日历
 * @see https://tushare.pro/document/2?doc_id=26
 */
type GetTradeCalParams = {
  exchange?: ExchangeCode // 交易所，默认SSE
  start_date: TsDate // 开始日期
  end_date: TsDate // 结束日期
}

type GetTradeCalItem = {
  exchange: ExchangeCode // 交易所
  cal_date: TsDate // 日期
  is_open: '0' | '1' // 是否交易 0休市 1交易
  pretrade_date?: TsDate // 上一个交易日
}

type GetTradeCalFields = keyof GetTradeCalItem

export const DefaultTradeCalFields: GetTradeCalFields[] = [
  'exchange', 'cal_date', 'is_open',
]

export type GetTradeCalResponse = ApiResponse<{
  fields: GetTradeCalFields[]
  items: GetTradeCalItem[GetTradeCalFields][][]
  has_more: boolean
  count: -1 // 这个字段在文档中没有描述，但是返回的实际数据中是-1
}>

type TushareApiMap = {
  'stock_basic': [GetStockBasicParams, GetStockBasicFields, GetStockBasicResponse]
  'daily': [GetDailyParams, GetDailyFields, GetDailyResponse]
  'trade_cal': [GetTradeCalParams, GetTradeCalFields, GetTradeCalResponse]
}

/**
 * Api请求限流：每分钟200次，每天100000次
 */
export default class Tushare {
  private rateLimiter: RateLimiter

  constructor(private token: string) {
    this.rateLimiter = new RateLimiter(60, 200)
  }

  private async _fetch<K extends keyof TushareApiMap, T = TushareApiMap[K][2], F = TushareApiMap[K][1]>(api_name: K, params: TushareApiMap[K][0], fields: F[]) {
    const url = `http://api.waditu.com`
    const response = await this.rateLimiter.execute(
      () => fetch(url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          api_name,
          token: this.token,
          params,
          fields,
        })
      }))
    return await response.json() as T
  }

  async fetchStockBasic(params: GetStockBasicParams = {
    list_status: 'L',
    limit: 10000,
  }, fields: GetStockBasicFields[] = [
    'ts_code', 'symbol', 'name', 'area', 'industry', 'cnspell', 'market', 'list_date', 'act_name', 'act_ent_type'
  ]) {
    return await this._fetch('stock_basic', params, fields)
  }

  async fetchDailyMd(params: GetDailyParams = {
    ts_code: '000001.SZ',
  }, fields: GetDailyFields[] = AllDailyFields) {
    return await this._fetch('daily', params, fields)
  }

  async fetchTradeCal(params: GetTradeCalParams, fields: GetTradeCalFields[] = DefaultTradeCalFields) {
    return await this._fetch('trade_cal', params, fields)
  }
}
