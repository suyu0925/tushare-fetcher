# tushare-fetcher

从tushare获取数据，并存储到数据库

## 准备

准备环境变量文件`.env`。

```bash
export TOKEN=your_tushare_token
export PGURL=postgresql://username:password@your.db.host:port/tushare?schema=public
```

## 使用

### 全量更新

第一次运行时，需要全量更新。

```bash
bun run scripts/full.ts
```

### 每日更新

tushare的A股数据在16:00后准备完成。

```bash
bun run scripts/daily.ts
```
