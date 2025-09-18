package com.tapdata.cdc.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/**
 * Loads external connection configuration for Elasticsearch and KingBase from
 * optional YAML files. If a file is provided via configuration (configFile) it
 * is preferred; otherwise we auto-detect files located next to the running JAR
 * (working directory) named 'elasticsearch.yml' and 'kingbase.yml'.
 */
@Component
public class ExternalConnectionConfigLoader {

    private static final Logger logger = LoggerFactory.getLogger(ExternalConnectionConfigLoader.class);

    private final ApplicationProperties applicationProperties;
    private final ResourceLoader resourceLoader;

    public ExternalConnectionConfigLoader(ApplicationProperties applicationProperties,
                                          ResourceLoader resourceLoader) {
        this.applicationProperties = applicationProperties;
        this.resourceLoader = resourceLoader;
    }

    @PostConstruct
    public void loadExternalConnectionConfigs() {
        tryLoadElasticsearch();
        tryLoadKingBase();
    }

    private void tryLoadElasticsearch() {
        ApplicationProperties.Elasticsearch es = applicationProperties.getElasticsearch();
        String location = es.getConfigFile();
        if (!StringUtils.hasText(location)) {
            // Default file next to the JAR
            location = "elasticsearch.yml";
        }
        Map<?, ?> yaml = loadYaml(location);
        if (yaml == null || yaml.isEmpty()) {
            return;
        }
        applyToElasticsearch(es, yaml);
    }

    private void tryLoadKingBase() {
        ApplicationProperties.KingBase kb = applicationProperties.getKingbase();
        String location = kb.getConfigFile();
        if (!StringUtils.hasText(location)) {
            // Default file next to the JAR
            location = "kingbase.yml";
        }
        Map<?, ?> yaml = loadYaml(location);
        if (yaml == null || yaml.isEmpty()) {
            return;
        }
        applyToKingBase(kb, yaml);
    }

    private Map<?, ?> loadYaml(String location) {
        try {
            Resource resource = resolveResource(location);
            if (resource == null || !resource.exists() || !resource.isReadable()) {
                return null;
            }
            try (InputStream in = resource.getInputStream()) {
                Yaml yaml = new Yaml();
                Object data = yaml.load(in);
                if (data instanceof Map) {
                    return (Map<?, ?>) data;
                }
            }
        } catch (Exception ex) {
            logger.warn("Failed to load external config '{}': {}", location, ex.getMessage());
        }
        return null;
    }

    private Resource resolveResource(String location) {
        String normalized = location.trim();

        // Prefer file system resource (absolute or relative to user.dir)
        Resource fs = resolveFileResource(normalized);
        if (fs != null && fs.exists() && fs.isReadable()) {
            return fs;
        }
        // Fallback to Spring's resource resolution (classpath:, file:, etc.)
        return resourceLoader.getResource(normalized);
    }

    private Resource resolveFileResource(String candidate) {
        if (!StringUtils.hasText(candidate)) {
            return null;
        }
        String pathText = candidate.startsWith("file:") ? candidate.substring("file:".length()) : candidate;
        Path path = Paths.get(pathText);
        if (!path.isAbsolute()) {
            path = Paths.get(System.getProperty("user.dir"), pathText);
        }
        try {
            Path abs = path.toAbsolutePath().normalize();
            if (!Files.exists(abs)) {
                return null;
            }
            return resourceLoader.getResource("file:" + abs.toString());
        } catch (Exception ignored) {
            return null;
        }
    }

    private void applyToElasticsearch(ApplicationProperties.Elasticsearch es, Map<?, ?> yaml) {
        for (Map.Entry<?, ?> e : yaml.entrySet()) {
            if (e.getKey() == null) continue;
            String key = normalizeKey(e.getKey().toString());
            Object val = e.getValue();
            try {
                if ("host".equals(key) && val != null) es.setHost(val.toString());
                else if ("port".equals(key)) es.setPort(asInt(val, es.getPort()));
                else if ("username".equals(key) && val != null) es.setUsername(val.toString());
                else if ("password".equals(key) && val != null) es.setPassword(val.toString());
            } catch (Exception ex) {
                logger.debug("Skip invalid elasticsearch config key '{}': {}", key, ex.getMessage());
            }
        }
        logger.info("Loaded external Elasticsearch connection config.");
    }

    private void applyToKingBase(ApplicationProperties.KingBase kb, Map<?, ?> yaml) {
        for (Map.Entry<?, ?> e : yaml.entrySet()) {
            if (e.getKey() == null) continue;
            String key = normalizeKey(e.getKey().toString());
            Object val = e.getValue();
            try {
                if ("host".equals(key) && val != null) kb.setHost(val.toString());
                else if ("port".equals(key)) kb.setPort(asInt(val, kb.getPort()));
                else if ("database".equals(key) && val != null) kb.setDatabase(val.toString());
                else if ("username".equals(key) && val != null) kb.setUsername(val.toString());
                else if ("password".equals(key) && val != null) kb.setPassword(val.toString());
                else if ("schema".equals(key) && val != null) kb.setSchema(val.toString());
                else if ("driverclassname".equals(key) && val != null) kb.setDriverClassName(val.toString());
                else if ("url".equals(key) && val != null) kb.setUrl(val.toString());
            } catch (Exception ex) {
                logger.debug("Skip invalid kingbase config key '{}': {}", key, ex.getMessage());
            }
        }
        logger.info("Loaded external KingBase connection config.");
    }

    private String normalizeKey(String s) {
        if (s == null) return null;
        return s.trim().toLowerCase(Locale.ROOT).replace("-", "").replace("_", "");
    }

    private int asInt(Object v, int fallback) {
        if (v == null) return fallback;
        if (v instanceof Number) return ((Number) v).intValue();
        try {
            return Integer.parseInt(v.toString());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }
}

