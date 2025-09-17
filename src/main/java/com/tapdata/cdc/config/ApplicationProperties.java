package com.tapdata.cdc.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@ConfigurationProperties(prefix = "tap")
public class ApplicationProperties {

    private final Elasticsearch elasticsearch = new Elasticsearch();
    private final Debezium debezium = new Debezium();
    private final KingBase kingbase = new KingBase();

    public Elasticsearch getElasticsearch() {
        return elasticsearch;
    }

    public Debezium getDebezium() {
        return debezium;
    }

    public KingBase getKingbase() {
        return kingbase;
    }

    public static class Elasticsearch {
        private String host = "localhost";
        private int port = 9200;
        private String username = "";
        private String password = "";

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class Debezium {
        private String serverName = "tapdata-cdc";
        private String tableIncludeList = "public.*";
        private String snapshotMode = "initial";
        private String pluginName = "pgoutput";
        private String offsetStorage = "org.apache.kafka.connect.storage.FileOffsetBackingStore";
        private String offsetStorageFile = "./offsets.dat";

        public String getServerName() {
            return serverName;
        }

        public void setServerName(String serverName) {
            this.serverName = serverName;
        }

        public String getTableIncludeList() {
            return tableIncludeList;
        }

        public void setTableIncludeList(String tableIncludeList) {
            this.tableIncludeList = tableIncludeList;
        }

        public String getSnapshotMode() {
            return snapshotMode;
        }

        public void setSnapshotMode(String snapshotMode) {
            this.snapshotMode = snapshotMode;
        }

        public String getPluginName() {
            return pluginName;
        }

        public void setPluginName(String pluginName) {
            this.pluginName = pluginName;
        }

        public String getOffsetStorage() {
            return offsetStorage;
        }

        public void setOffsetStorage(String offsetStorage) {
            this.offsetStorage = offsetStorage;
        }

        public String getOffsetStorageFile() {
            return offsetStorageFile;
        }

        public void setOffsetStorageFile(String offsetStorageFile) {
            this.offsetStorageFile = offsetStorageFile;
        }
    }

    public static class KingBase {
        private boolean enabled = false;
        private boolean pollingEnabled = false;
        private boolean sqlSyncEnabled = false;
        private String host = "localhost";
        private int port = 54321;
        private String database = "ai_assistant";
        private String username = "kingbase";
        private String password = "kingbase";
        private String schema = "public";
        private String driverClassName = "com.kingbase8.Driver";
        private String url = "";
        private String sqlStatementsFile = "";
        private String sqlSyncCron = "0 */5 * * * *";
        private int statementParallelism = 4;
        private int defaultChunkSize = 0;
        private int defaultFetchSize = 1000;
        private String syncStateTable = "kingbase_sync_state";
        private List<SqlStatement> sqlStatements = new ArrayList<SqlStatement>();
        private List<StatementGroup> sqlStatementGroups = new ArrayList<StatementGroup>();

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isPollingEnabled() {
            return pollingEnabled;
        }

        public void setPollingEnabled(boolean pollingEnabled) {
            this.pollingEnabled = pollingEnabled;
        }

        public boolean isSqlSyncEnabled() {
            return sqlSyncEnabled;
        }

        public void setSqlSyncEnabled(boolean sqlSyncEnabled) {
            this.sqlSyncEnabled = sqlSyncEnabled;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getDriverClassName() {
            return driverClassName;
        }

        public void setDriverClassName(String driverClassName) {
            this.driverClassName = driverClassName;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getSqlSyncCron() {
            return sqlSyncCron;
        }

        public void setSqlSyncCron(String sqlSyncCron) {
            this.sqlSyncCron = sqlSyncCron;
        }

        public String getSqlStatementsFile() {
            return sqlStatementsFile;
        }

        public void setSqlStatementsFile(String sqlStatementsFile) {
            this.sqlStatementsFile = sqlStatementsFile;
        }

        public int getStatementParallelism() {
            return statementParallelism;
        }

        public void setStatementParallelism(int statementParallelism) {
            this.statementParallelism = statementParallelism;
        }

        public int getDefaultChunkSize() {
            return defaultChunkSize;
        }

        public void setDefaultChunkSize(int defaultChunkSize) {
            this.defaultChunkSize = defaultChunkSize;
        }

        public int getDefaultFetchSize() {
            return defaultFetchSize;
        }

        public void setDefaultFetchSize(int defaultFetchSize) {
            this.defaultFetchSize = defaultFetchSize;
        }

        public String getSyncStateTable() {
            return syncStateTable;
        }

        public void setSyncStateTable(String syncStateTable) {
            this.syncStateTable = syncStateTable;
        }

        public List<SqlStatement> getSqlStatements() {
            return sqlStatements;
        }

        public void setSqlStatements(List<SqlStatement> sqlStatements) {
            if (sqlStatements == null) {
                this.sqlStatements = new ArrayList<SqlStatement>();
            } else {
                this.sqlStatements = sqlStatements;
            }
        }

        public List<StatementGroup> getSqlStatementGroups() {
            return sqlStatementGroups;
        }

        public void setSqlStatementGroups(List<StatementGroup> sqlStatementGroups) {
            if (sqlStatementGroups == null) {
                this.sqlStatementGroups = new ArrayList<StatementGroup>();
            } else {
                this.sqlStatementGroups = sqlStatementGroups;
            }
        }

        public static class SqlStatement {
            private String name;
            private String sql;
            private String index;
            private String idColumn = "id";
            private boolean incremental = true;
            private boolean enabled = true;
            private Integer chunkSize;
            private Integer fetchSize;
            private Boolean streamResults;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public String getSql() {
                return sql;
            }

            public void setSql(String sql) {
                this.sql = sql;
            }

            public String getIndex() {
                return index;
            }

            public void setIndex(String index) {
                this.index = index;
            }

            public String getIdColumn() {
                return idColumn;
            }

            public void setIdColumn(String idColumn) {
                this.idColumn = idColumn;
            }

            public boolean isIncremental() {
                return incremental;
            }

            public void setIncremental(boolean incremental) {
                this.incremental = incremental;
            }

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public Integer getChunkSize() {
                return chunkSize;
            }

            public void setChunkSize(Integer chunkSize) {
                this.chunkSize = chunkSize;
            }

            public Integer getFetchSize() {
                return fetchSize;
            }

            public void setFetchSize(Integer fetchSize) {
                this.fetchSize = fetchSize;
            }

            public Boolean getStreamResults() {
                return streamResults;
            }

            public void setStreamResults(Boolean streamResults) {
                this.streamResults = streamResults;
            }
        }

        public static class StatementGroup {
            private String name;
            private String file;
            private String cron;

            public String getName() {
                return name;
            }

            public void setName(String name) {
                this.name = name;
            }

            public String getFile() {
                return file;
            }

            public void setFile(String file) {
                this.file = file;
            }

            public String getCron() {
                return cron;
            }

            public void setCron(String cron) {
                this.cron = cron;
            }
        }
    }
}
