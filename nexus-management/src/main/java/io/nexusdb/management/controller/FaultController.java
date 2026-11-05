package io.nexusdb.management.controller;

import io.nexusdb.management.service.ClusterService;
import io.nexusdb.management.service.MetadataService;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
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
@RequestMapping("/api/faults")
public class FaultController {

    public record FaultRequest(String targetNode, String action, int durationMs) {}

    private final ClusterService clusterService;
    private final MetadataService metadataService;

    public FaultController(ClusterService clusterService, MetadataService metadataService) {
        this.clusterService = clusterService;
        this.metadataService = metadataService;
    }

    @PostMapping("/action")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<?> executeFaultAction(@RequestBody FaultRequest request, Authentication authentication) {
        String username = authentication != null ? authentication.getName() : "admin";
        boolean success = clusterService.injectFault(request.targetNode(), request.action(), request.durationMs());

        String details = "Action=" + request.action() + ", Duration=" + request.durationMs() + "ms, Result=" + (success ? "SUCCESS" : "FAILED");
        metadataService.recordAudit(username, "FAULT_INJECTION", request.targetNode(), details);

        return ResponseEntity.ok(Map.of(
                "success", success,
                "targetNode", request.targetNode(),
                "action", request.action(),
                "status", success ? "EXECUTED" : "FAILED"
        ));
    }

    @GetMapping("/audit-log")
    public ResponseEntity<List<Map<String, Object>>> getAuditLog(@RequestParam(value = "limit", defaultValue = "50") int limit) {
        return ResponseEntity.ok(metadataService.getAuditLogs(limit));
    }
}
