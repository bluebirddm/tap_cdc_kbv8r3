package com.tapdata.cdc.config;

import com.tapdata.cdc.config.ApplicationProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;

/**
 * Provides a dedicated KingBase {@link DataSource} and {@link JdbcTemplate} when enabled.
 */
@Configuration
@ConditionalOnProperty(prefix = "tap.kingbase", name = "enabled", havingValue = "true")
public class KingBaseDataSourceConfig {

    private final ApplicationProperties applicationProperties;

    public KingBaseDataSourceConfig(ApplicationProperties applicationProperties) {
        this.applicationProperties = applicationProperties;
    }

    @Bean
    public DataSource kingbaseDataSource() {
        ApplicationProperties.KingBase kingbase = applicationProperties.getKingbase();
        DataSourceBuilder<?> builder = DataSourceBuilder.create();

        String url = kingbase.getUrl();
        if (!StringUtils.hasText(url)) {
            url = String.format(
                "jdbc:kingbase8://%s:%d/%s",
                kingbase.getHost(),
                kingbase.getPort(),
                kingbase.getDatabase()
            );
        }

        builder.url(url);
        builder.username(kingbase.getUsername());
        builder.password(kingbase.getPassword());

        if (StringUtils.hasText(kingbase.getDriverClassName())) {
            builder.driverClassName(kingbase.getDriverClassName());
        }

        return builder.build();
    }

    @Bean
    public JdbcTemplate kingbaseJdbcTemplate(@Qualifier("kingbaseDataSource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
