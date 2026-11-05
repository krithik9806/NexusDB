package io.nexusdb.management.controller;

import io.nexusdb.management.service.ClusterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class SchemaController {

    private final ClusterService clusterService;

    public SchemaController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping("/schema")
    public ResponseEntity<List<Map<String, Object>>> getSchema() {
        return ResponseEntity.ok(clusterService.getSchema());
    }
}
