package io.nexusdb.management.controller;

import io.nexusdb.management.service.MetricsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/metrics")
public class MetricsController {

    private final MetricsService metricsService;

    public MetricsController(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    @GetMapping("/timeseries")
    public ResponseEntity<List<MetricsService.MetricPoint>> getTimeSeries() {
        return ResponseEntity.ok(metricsService.getTimeSeries());
    }

    @GetMapping("/latest")
    public ResponseEntity<MetricsService.MetricPoint> getLatest() {
        return ResponseEntity.ok(metricsService.getLatest());
    }
}
