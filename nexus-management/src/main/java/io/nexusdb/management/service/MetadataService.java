package io.nexusdb.management.service;

import jakarta.annotation.PostConstruct;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Clob;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Service
public class MetadataService {

    private final JdbcTemplate jdbcTemplate;

    public MetadataService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @PostConstruct
    public void initDb() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS query_history (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(100) NOT NULL,
                sql_query TEXT NOT NULL,
                execution_time_ms BIGINT NOT NULL,
                rows_affected INT NOT NULL,
                status VARCHAR(20) NOT NULL,
                error_message TEXT,
                executed_at TIMESTAMP NOT NULL
            )
        """);

        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(100) NOT NULL,
                action VARCHAR(100) NOT NULL,
                target_node VARCHAR(100),
                details TEXT,
                created_at TIMESTAMP NOT NULL
            )
        """);
    }

    public void recordQueryHistory(String username, String sql, long executionTimeMs, int rowsAffected, String status, String error) {
        jdbcTemplate.update("""
            INSERT INTO query_history (username, sql_query, execution_time_ms, rows_affected, status, error_message, executed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, username, sql, executionTimeMs, rowsAffected, status, error, Timestamp.from(Instant.now()));
    }

    public List<Map<String, Object>> getRecentQueryHistory(int limit) {
        String sql = """
            SELECT id, username, sql_query, execution_time_ms, rows_affected, status, error_message, executed_at
            FROM query_history
            ORDER BY executed_at DESC
            LIMIT ?
        """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> map = new LinkedHashMap<>();
            var meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                Object val = rs.getObject(i);
                if (val instanceof Clob clob) {
                    try {
                        val = clob.getSubString(1, (int) clob.length());
                    } catch (Exception ignored) {
                        val = "";
                    }
                }
                map.put(meta.getColumnLabel(i).toLowerCase(Locale.ROOT), val);
            }
            return map;
        }, limit);
    }

    public void recordAudit(String username, String action, String targetNode, String details) {
        jdbcTemplate.update("""
            INSERT INTO audit_log (username, action, target_node, details, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, username, action, targetNode, details, Timestamp.from(Instant.now()));
    }

    public List<Map<String, Object>> getAuditLogs(int limit) {
        String sql = """
            SELECT id, username, action, target_node, details, created_at
            FROM audit_log
            ORDER BY created_at DESC
            LIMIT ?
        """;
        return jdbcTemplate.query(sql, (rs, rowNum) -> {
            Map<String, Object> map = new LinkedHashMap<>();
            var meta = rs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                Object val = rs.getObject(i);
                if (val instanceof Clob clob) {
                    try {
                        val = clob.getSubString(1, (int) clob.length());
                    } catch (Exception ignored) {
                        val = "";
                    }
                }
                map.put(meta.getColumnLabel(i).toLowerCase(Locale.ROOT), val);
            }
            return map;
        }, limit);
    }
}
