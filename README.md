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
```

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

- **热更新提示**：打包成可执行 JAR 后，可将 `sql-statements.yml` 或 `sql-statements-large.yml` 拷贝到与 JAR 相同的目录。应用会优先读取这些外部文件，其次才回退到包内的 classpath 资源。

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
