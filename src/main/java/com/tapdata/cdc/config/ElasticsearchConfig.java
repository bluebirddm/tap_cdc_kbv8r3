package com.tapdata.cdc.config;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    @Autowired
    private ApplicationProperties properties;

    @Bean(destroyMethod = "close")
    public RestHighLevelClient elasticsearchClient() {
        ApplicationProperties.Elasticsearch esConfig = properties.getElasticsearch();

        RestClientBuilder builder = RestClient.builder(new HttpHost(esConfig.getHost(), esConfig.getPort(), "http"));

        if (hasText(esConfig.getUsername()) && hasText(esConfig.getPassword())) {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(esConfig.getUsername(), esConfig.getPassword()));

            builder.setHttpClientConfigCallback(httpClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            );
        }

        builder.setRequestConfigCallback(requestConfigBuilder ->
            requestConfigBuilder
                .setConnectTimeout(5000)
                .setSocketTimeout(30000)
        );

        return new RestHighLevelClient(builder);
    }

    private boolean hasText(String value) {
        return value != null && !value.trim().isEmpty();
    }
}
