# debezium-dameng

#### 介绍
金仓数据库同步工具，使用基于日志的CDC

#### 软件架构
基于debezium 1.5.4final和金仓的日志解析工具API

###### 本地测试

- 确保 Maven 能加载 Kingbase JDBC 驱动：`pom.xml` 默认指向 `/opt/kingbase_bare_metal/ES/V8/Jdbc/jdk/kingbase8-8.2.0.jar`，如果安装路径不同，可通过 `-Dkingbase.jdbc.jar=/your/path/kingbase8.jar` 覆盖。
- 在 `sys_hba.conf` 中允许 Debezium 账户进行逻辑复制，例如追加 `host    replication    debezium_user    127.0.0.1/32    trust`（或使用实际客户端地址 172.17.0.1），修改后重启 Kingbase。
- 运行 `mvn -q -Dexec.classpathScope=test -Dexec.mainClass=KingbaseTest exec:java` 验证，首次执行会完成初始化快照。

#### 集成到Flink CDC






