package com.tapdata.cdc.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.util.StringUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Allows overriding selected application properties via environment variables,
 * system properties, or external YAML files located next to the executable JAR
 * before Spring binds configuration properties.
 */
public class AppPropertiesEnvironmentPostProcessor implements EnvironmentPostProcessor {

    private static final Logger logger = LoggerFactory.getLogger(AppPropertiesEnvironmentPostProcessor.class);

    private static final String ES_PREFIX = "tap.elasticsearch.";
    private static final String KB_PREFIX = "tap.kingbase.";
    private static final String ES_DEFAULT_FILE = "elasticsearch.yml";
    private static final String KB_DEFAULT_FILE = "kingbase.yml";

    private static final Map<String, String> ES_KEYS = createElasticsearchMapping();
    private static final Map<String, String> KB_KEYS = createKingbaseMapping();

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Map<String, Object> overrides = new HashMap<String, Object>();

        applyEnvOverride(overrides, "TAP_ELASTICSEARCH_BULK_SIZE", ES_PREFIX + "bulk-size");
        applyEnvOverride(overrides, "TAP_ELASTICSEARCH_BULK_FLUSH_INTERVAL_SECONDS", ES_PREFIX + "bulk-flush-interval-seconds");
        applyEnvOverride(overrides, "TAP_ELASTICSEARCH_BULK_THREADS", ES_PREFIX + "bulk-threads");

        applyExternalYaml(environment, overrides, ES_PREFIX, ES_KEYS, ES_DEFAULT_FILE);
        applyExternalYaml(environment, overrides, KB_PREFIX, KB_KEYS, KB_DEFAULT_FILE);

        if (!overrides.isEmpty()) {
            environment.getPropertySources().addFirst(new MapPropertySource("tap-connection-overrides", overrides));
            logger.info("Applied connection overrides: {}", overrides);
        }
    }

    private void applyEnvOverride(Map<String, Object> overrides, String envName, String propertyKey) {
        String value = System.getenv(envName);
        if (!StringUtils.hasText(value)) {
            value = System.getProperty(propertyKey);
        }
        if (StringUtils.hasText(value)) {
            overrides.put(propertyKey, value);
        }
    }

    private void applyExternalYaml(ConfigurableEnvironment environment,
                                   Map<String, Object> overrides,
                                   String prefix,
                                   Map<String, String> mapping,
                                   String defaultFile) {
        String configuredLocation = environment.getProperty(prefix + "config-file");
        List<InputStream> candidates = new ArrayList<InputStream>();
        List<String> descriptions = new ArrayList<String>();

        if (StringUtils.hasText(configuredLocation)) {
            InputStream stream = openResource(configuredLocation);
            if (stream != null) {
                candidates.add(stream);
                descriptions.add(configuredLocation);
            }
        }

        InputStream defaultStream = openResource(defaultFile);
        if (defaultStream != null) {
            candidates.add(defaultStream);
            descriptions.add(defaultFile);
        }

        for (int i = 0; i < candidates.size(); i++) {
            try (InputStream in = candidates.get(i)) {
                Yaml yaml = new Yaml();
                Object loaded = yaml.load(in);
                if (!(loaded instanceof Map<?, ?>)) {
                    logger.warn("Config file '{}' does not contain a YAML object", descriptions.get(i));
                    continue;
                }
                applyYamlMap(overrides, mapping, (Map<?, ?>) loaded);
                logger.info("Loaded connection overrides from '{}'.", descriptions.get(i));
                break; // first successful load wins
            } catch (IOException ex) {
                logger.warn("Failed to read config file '{}': {}", descriptions.get(i), ex.getMessage());
            }
        }
    }

    private InputStream openResource(String location) {
        if (!StringUtils.hasText(location)) {
            return null;
        }

        String normalized = location.trim();
        try {
            if (normalized.startsWith("classpath:")) {
                String resourcePath = normalized.substring("classpath:".length());
                URL url = getClass().getClassLoader().getResource(resourcePath);
                if (url != null) {
                    return url.openStream();
                }
                return null;
            }

            if (normalized.startsWith("file:")) {
                normalized = normalized.substring("file:".length());
            }

            Path path = Paths.get(normalized);
            if (!path.isAbsolute()) {
                path = Paths.get(System.getProperty("user.dir"), normalized);
            }
            if (Files.exists(path)) {
                return Files.newInputStream(path);
            }
        } catch (IOException ex) {
            logger.warn("Failed to open resource '{}': {}", location, ex.getMessage());
        }
        return null;
    }

    private void applyYamlMap(Map<String, Object> overrides, Map<String, String> mapping, Map<?, ?> map) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (entry.getKey() == null) {
                continue;
            }
            String key = normalizeKey(entry.getKey().toString());
            String property = mapping.get(key);
            if (property == null) {
                logger.debug("Ignoring unknown config key '{}' in external file", key);
                continue;
            }
            if (overrides.containsKey(property)) {
                continue; // env/system property takes precedence
            }
            overrides.put(property, entry.getValue());
        }
    }

    private String normalizeKey(String key) {
        return key.trim().toLowerCase(Locale.ROOT).replace("-", "").replace("_", "");
    }

    private static Map<String, String> createElasticsearchMapping() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("host", ES_PREFIX + "host");
        map.put("port", ES_PREFIX + "port");
        map.put("username", ES_PREFIX + "username");
        map.put("password", ES_PREFIX + "password");
        map.put("bulksize", ES_PREFIX + "bulk-size");
        map.put("bulkflushintervalseconds", ES_PREFIX + "bulk-flush-interval-seconds");
        map.put("bulkthreads", ES_PREFIX + "bulk-threads");
        return map;
    }

    private static Map<String, String> createKingbaseMapping() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("host", KB_PREFIX + "host");
        map.put("port", KB_PREFIX + "port");
        map.put("database", KB_PREFIX + "database");
        map.put("username", KB_PREFIX + "username");
        map.put("password", KB_PREFIX + "password");
        map.put("schema", KB_PREFIX + "schema");
        map.put("driverclassname", KB_PREFIX + "driver-class-name");
        map.put("url", KB_PREFIX + "url");
        return map;
    }
}
