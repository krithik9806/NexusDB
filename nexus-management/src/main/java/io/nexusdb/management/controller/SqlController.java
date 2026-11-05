package io.nexusdb.management.controller;

import io.nexusdb.management.service.ClusterService;
import io.nexusdb.management.service.MetadataService;
import io.nexusdb.management.service.MetricsService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/sql")
public class SqlController {

    public record ExecuteRequest(String sql) {}

    private final ClusterService clusterService;
    private final MetadataService metadataService;
    private final MetricsService metricsService;

    public SqlController(ClusterService clusterService,
                         MetadataService metadataService,
                         MetricsService metricsService) {
        this.clusterService = clusterService;
        this.metadataService = metadataService;
        this.metricsService = metricsService;
    }

    @PostMapping("/execute")
    public ResponseEntity<?> executeSql(@RequestBody ExecuteRequest request, Authentication authentication) {
        String username = authentication != null ? authentication.getName() : "anonymous";
        ClusterService.SqlExecutionResult result = clusterService.executeSql(request.sql());
        metricsService.incrementQueryCount();

        String status = result.error() == null ? "SUCCESS" : "ERROR";
        metadataService.recordQueryHistory(
                username,
                request.sql(),
                result.executionTimeMs(),
                result.rowsAffected(),
                status,
                result.error()
        );

        if (result.error() != null) {
            return ResponseEntity.badRequest().body(Map.of(
                    "error", result.error(),
                    "executionTimeMs", result.executionTimeMs()
            ));
        }

        return ResponseEntity.ok(Map.of(
                "columns", result.columns(),
                "rows", result.rows(),
                "rowsAffected", result.rowsAffected(),
                "executionTimeMs", result.executionTimeMs()
        ));
    }

    @PostMapping("/explain")
    public ResponseEntity<?> explainSql(@RequestBody ExecuteRequest request) {
        String explanation = clusterService.explainSql(request.sql());
        return ResponseEntity.ok(Map.of("explanation", explanation));
    }

    @GetMapping("/history")
    public ResponseEntity<List<Map<String, Object>>> getQueryHistory(@RequestParam(value = "limit", defaultValue = "50") int limit) {
        return ResponseEntity.ok(metadataService.getRecentQueryHistory(limit));
    }
}
