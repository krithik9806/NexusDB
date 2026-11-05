package io.nexusdb.management.controller;

import io.nexusdb.management.service.ClusterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api")
public class ClusterController {

    private final ClusterService clusterService;

    public ClusterController(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @GetMapping("/cluster/topology")
    public ResponseEntity<List<ClusterService.NodeInfo>> getTopology() {
        return ResponseEntity.ok(clusterService.getClusterTopology());
    }

    @GetMapping("/nodes/{id}")
    public ResponseEntity<ClusterService.NodeInfo> getNodeDetails(@PathVariable("id") String id) {
        return ResponseEntity.ok(clusterService.getNodeDetails(id));
    }
}
