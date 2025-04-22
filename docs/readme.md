# RusticDB 設計文件（MVP）

## 1. 資料儲存

- 固定大小頁面（4KB）組成資料檔案
- `Pager`: 提供頁面讀寫 API，負責 cache/page id 映射
- 頁面類型：Free / Table Heap / B+ Tree Node / WAL

## 2. 表格與索引

- `Table`：透過 heap 儲存 row 資料（固定長度）
- `BPlusTree`：索引頁面，用於加速主鍵查詢
- 行格式：使用定長 header + 可變 body（預設不壓縮）

## 3. SQL 執行

- 支援 `CREATE TABLE`, `INSERT`, `SELECT` (WHERE主鍵), `BEGIN`/`COMMIT`
- `sqlparser-rs`：解析 SQL → Plan
- `Executor`：對應不同 Plan node 執行查詢

## 4. 持久化

- `WriteAheadLog`：實作 redo log、簡易 crash recovery
- 使用順序寫入 + checkpoint 機制

## 5. CLI 介面

- 用 `rusticdb-cli` 接收 REPL 輸入
- 顯示 query 結果、錯誤訊息
