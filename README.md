# debezium-dameng

#### 介绍
金仓数据库同步工具，使用基于日志的CDC

#### 软件架构
基于 debezium 1.5.4.Final 和金仓的日志解析工具 API

## KingBase SQL 同步服务

基于 `KingBaseSqlSyncService` 的批量同步用于将自定义查询的结果推送到 Elasticsearch。服务具备以下特性：

- **分块与流式处理**：通过 `chunk-size` 与 `fetch-size` 控制单次批量的行数，避免一次性装载巨量数据。
- **并行执行**：`tap.kingbase.statement-parallelism` 定义了可同时运行的 SQL 语句数量。
- **游标持久化**：每条语句的最新 ID 会持久化在 `tap.kingbase.sync-state-table` 指定的表中，重启后自动增量续跑。
- **启动即同步**：应用启动后会自动提交一次同步任务，之后按 `tap.kingbase.sql-sync-cron` 设定的 cron 表达式周期执行。

### 配置示例

`src/main/resources/application.yml` 中提供了默认示例，可按需调整：

```yaml
tap:
  kingbase:
    config-file: kingbase.yml
    enabled: true
    sql-sync-enabled: true
    sql-sync-cron: "0 */15 * * * *"   # 每 15 分钟执行一次
    statement-parallelism: 4
    default-chunk-size: 0              # 0 表示未指定，使用 SQL 自带 LIMIT
    default-fetch-size: 1000
    sync-state-table: kingbase_sync_state
    sql-statements-file: classpath:sql-statements.yml
    sql-statement-groups:
      - name: large-orders
        file: classpath:sql-statements-large.yml
        cron: "0 0 * * * *"           # 大型表每天整点跑一次
  elasticsearch:
    config-file: elasticsearch.yml
    host: localhost
    port: 9200
    username: elastic
    password: changeme
    bulk-size: 2000
    bulk-flush-interval-seconds: 5
    bulk-threads: 2
```

### 外部连接配置

- 在 JAR 同目录放置 `kingbase.yml`、`elasticsearch.yml` 可覆盖连接信息和批量参数，例如：

`kingbase.yml`
```yaml
host: localhost
port: 54321
database: TEST
username: SYSTEM
password: 123456
schema: PUBLIC
driver-class-name: com.kingbase8.Driver
url: ""
```

`elasticsearch.yml`
```yaml
host: localhost
port: 9200
username: elastic
password: changeme
bulk-size: 2000
bulk-flush-interval-seconds: 5
bulk-threads: 2
```

- 也可通过 `tap.kingbase.config-file` 与 `tap.elasticsearch.config-file` 指定绝对路径。

### 定义 SQL 语句

将语句放在 `src/main/resources/sql-statements.yml`，按名称组织成键值对：

```yaml
sample_orders_sync:
  sql: "SELECT * FROM orders WHERE id > ? ORDER BY id LIMIT :chunkSize"
  index: kingbase_orders
  id-column: id
  chunk-size: 5000
  fetch-size: 5000
  stream-results: true
```

字段说明：

- `sql`：必须包含按游标递增的 WHERE 条件，例如 `id > ?`，并可使用 `:chunkSize` 占位符。
- `index`：Elasticsearch 索引名称。
- `id-column`：结果集中用于生成文档 ID 的字段。
- `id-type`：游标类型，`number`（默认）或 `string`。当为 `string` 时，增量比较与数据库的字符串排序一致，请确保该列为单调递增且已建索引。
- `chunk-size` / `fetch-size` / `stream-results`：覆盖默认的批量控制参数。

对于特别大的表，可以单独放入 `sql-statements-large.yml`，并在 `sql-statement-groups` 中指定独立的 cron 表达式：

```yaml
large_orders_sync:
  sql: "SELECT * FROM orders WHERE id > ? ORDER BY id LIMIT :chunkSize"
  index: kingbase_orders_large
  id-column: id
  chunk-size: 10000
  fetch-size: 10000
  stream-results: true
```

### 运行同步

1. 配置 KingBase JDBC 数据源、Elasticsearch 连接信息及上述 SQL 文件。
2. 执行 `mvn spring-boot:run` 或通过容器方式启动应用。
3. 启动日志会提示首次同步已提交，后续按 cron 周期执行；观察 `kingbase_sync_state` 表可验证游标更新。

### 管理 API

应用启动后可通过以下接口进行运维（默认端口 8080）：

- 触发一次全局同步：
  `curl -X POST http://localhost:8080/kingbase/sync`
- 清空游标表（`tap.kingbase.sync-state-table`）：
  `curl -X DELETE http://localhost:8080/kingbase/sync-state`
- 重新装载并计划语句分组（修改外部 YAML 后调用）：
  `curl -X PUT http://localhost:8080/kingbase/sync-groups/refresh`

### 搜索 API

`SearchController` 暴露在 `http://<host>:<port>/api/search` 下，默认端口为 `8080`。分页参数 `page` 从 `0` 开始计数，`size` 取值范围为 `1`~`100`，未指定时使用默认值 `page=0`、`size=20`。

- 全局搜索（在所有索引中按关键字搜索，可省略 `keyword` 返回全部；响应包含 `_source` 字段，体积最大）：
  ```bash
  curl "http://localhost:8080/api/search/global?keyword=order&page=0&size=20"
  ```
- 全局搜索（仅返回索引与文档 ID，适用于只需定位文档时；ID 会自动移除 `src/main/resources/sql-statements.yml` 中语句名称生成的前缀，以便直接映射源表主键）：
  ```bash
  curl "http://localhost:8080/api/search/global/ids?keyword=order&page=0&size=20"
  ```
- 指定 schema + table 搜索（`schema` 与 `table` 为必填，其余参数同上）：
  ```bash
  curl "http://localhost:8080/api/search/table?schema=PUBLIC&table=orders&keyword=order&page=0&size=20"
  ```
- 指定 schema 下的所有表搜索（`schema` 必填，`keyword` 可选）：
  ```bash
  curl "http://localhost:8080/api/search/schema?schema=PUBLIC&keyword=order&page=0&size=20"
  ```
- 查询可用索引列表：
  ```bash
  curl "http://localhost:8080/api/search/indices"
  ```
- 高级搜索（POST JSON 请求体，可同时限定 `schema`/`table` 与分页）：
  ```bash
  curl -X POST "http://localhost:8080/api/search/advanced" \
       -H "Content-Type: application/json" \
       -d '{
             "keyword": "order",
             "schema": "PUBLIC",
             "table": "orders",
             "page": 0,
             "size": 20
           }'
  ```
  请求体字段均可省略，未提供时将回退到默认值；例如去掉 `schema`/`table` 即等价于全局搜索。

### SQL 转换工具

应用内置了一个 SQL 转换工具，用于将标准 SQL 语句转换为 Kingbase 兼容格式。该工具可自动添加 Schema 前缀、转义标识符，并处理表名映射。

#### 访问方式
启动应用后，访问：`http://localhost:8080/sql-transform`

#### 功能特性
- **自动转换**：实时将标准 SQL 转换为 Kingbase 格式
- **Schema 前缀**：自动添加可配置的 Schema 前缀
- **标识符转义**：自动为表名和列名添加双引号
- **表名映射**：支持自定义表名转换规则（如 `orders` → `T_JW_RYCW`）
- **列名大写**：自动将列名转换为大写
- **参数保留**：保留 SQL 中的参数占位符（`?` 和 `:namedParam`）
- **离线运行**：无需网络连接，完全本地化

#### 使用说明
1. 在左侧输入框中输入标准 SQL 语句
2. 系统会实时在右侧显示转换后的 Kingbase SQL
3. 可以自定义 Schema 前缀和表名映射规则
4. 点击"复制"按钮将结果复制到剪贴板
5. 配置会自动保存在浏览器本地存储中

### 同步游标提示

- 若 ID 列为字符串类型，针对 KingBase 的比较 `WHERE "ID" > ''` 不会命中任何记录。应用首次运行时会自动使用空格 `' '` 作为初始游标；如需重置增量状态，请先 `DELETE FROM kingbase_sync_state;`，随后在首次查询时仍使用 `' '` 作为游标。

- 若 ID 列为字符串类型，针对 KingBase 的比较 `WHERE "ID" > ''` 不会命中任何记录。应用首次运行时会自动使用空格 `' '` 作为初始游标；如果手动触发或清空状态表，请确保游标初值为 `' '`，或在清理 `kingbase_sync_state` 后重新启动。

### 控制日志量

- 批量入队日志采样：为避免高表量导致 DEBUG 日志过多，`ElasticsearchService` 对“Queued ... operation”日志做了采样，默认每 1000 条打印 1 次。
- 通过 JVM 系统属性调整采样频率（数值越大，日志越少）：
  - 运行 JAR：`java -Dtap.es.logEvery=5000 -jar target/debezium-kingbase-1.0-SNAPSHOT.jar`
  - Spring Boot 运行：`mvn spring-boot:run -Dspring-boot.run.jvmArguments="-Dtap.es.logEvery=5000"`
- 也可以直接提高类日志级别来完全关闭该类的 DEBUG：
  ```yaml
  logging:
    level:
      com.tapdata.cdc.elasticsearch.ElasticsearchService: INFO
  ```

### Elasticsearch 批量参数

- `tap.elasticsearch.bulk-size`：单批次最多提交的操作数（默认 1000）。
- `tap.elasticsearch.bulk-flush-interval-seconds`：后台定时刷新间隔（默认 5 秒）。
- `tap.elasticsearch.bulk-threads`：用于 bulk 调度的线程数量（默认 2）。
- 可在 JAR 同目录放置 `elasticsearch.yml` 覆盖以上及连接信息，例如：

```yaml
host: localhost
port: 9200
username: elastic
password: changeme
bulk-size: 5000
bulk-flush-interval-seconds: 3
bulk-threads: 4
```

- 日志中会包含各索引的操作数量，便于定位热点索引。

### 处理大型表

- 每条语句都有独立锁。若上一轮执行仍未完成，后续触发只会跳过该语句，其他语句仍会按预定周期运行。
- 将大型表拆分为带有不同 `chunk-size` 参数的多条语句，或放在独立的 YAML 文件中以便单独调度。
- 关注执行日志：当语句被跳过时会打印 `Skipping SQL statement '<name>' because a previous execution is still running.`，据此调优 `chunk-size`、`fetch-size` 或 `sql-sync-cron`。

###### 本地测试

- 确保 Maven 能加载 Kingbase JDBC 驱动：`pom.xml` 默认指向 `/opt/kingbase_bare_metal/ES/V8/Jdbc/jdk/kingbase8-8.2.0.jar`，如果安装路径不同，可通过 `-Dkingbase.jdbc.jar=/your/path/kingbase8.jar` 覆盖。
- 在 `sys_hba.conf` 中允许 Debezium 账户进行逻辑复制，例如追加 `host    replication    debezium_user    127.0.0.1/32    trust`（或使用实际客户端地址 172.17.0.1），修改后重启 Kingbase。
- 运行 `mvn -q -Dexec.classpathScope=test -Dexec.mainClass=KingbaseTest exec:java` 验证，首次执行会完成初始化快照。


Use your app’s base URL and send simple HTTP calls—assuming it’s running locally on port 8080:

- Trigger the sync immediately
  curl -X POST http://localhost:8080/kingbase/sync
- Clear every row in the configured sync-state table
  curl -X DELETE http://localhost:8080/kingbase/sync-state
- Rebuild the scheduled statement groups (use after editing group YAML)
  curl -X PUT http://localhost:8080/kingbase/sync-groups/refresh

Add -H "Content-Type: application/json" only if you start sending JSON payloads (not required for these endpoints).
