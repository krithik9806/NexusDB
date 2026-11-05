import React, { useEffect, useState } from 'react';
import { ApiClient, MetricPoint } from '../api/client';
import { Zap, Clock, Database, HardDrive, RefreshCw } from 'lucide-react';

export const PerformanceCenterView: React.FC = () => {
  const [metrics, setMetrics] = useState<MetricPoint[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchMetrics = async () => {
    try {
      setLoading(true);
      const data = await ApiClient.getTimeSeries();
      setMetrics(data);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 3000);
    return () => clearInterval(interval);
  }, []);

  const latest = metrics.length > 0 ? metrics[metrics.length - 1] : null;

  // Simple clean SVG line chart generator
  const renderLineChart = (
    data: number[],
    color: string,
    unit: string,
    label: string,
    height: number = 120
  ) => {
    if (data.length < 2) {
      return (
        <div className="h-28 flex items-center justify-center text-xs text-slate-500 font-mono">
          Accumulating time series data points...
        </div>
      );
    }

    const min = Math.min(...data);
    const max = Math.max(...data, min + 0.001);
    const range = max - min;
    const width = 450;
    const points = data
      .map((val, idx) => {
        const x = (idx / (data.length - 1)) * width;
        const y = height - ((val - min) / range) * (height - 20) - 10;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(' ');

    return (
      <div className="relative pt-2">
        <div className="flex justify-between text-[11px] font-mono text-slate-400 mb-1">
          <span>{label}</span>
          <span className="text-white font-bold">
            {data[data.length - 1].toFixed(2)} {unit}
          </span>
        </div>
        <svg viewBox={`0 0 ${width} ${height}`} className="w-full h-28 overflow-visible">
          <polyline
            fill="none"
            stroke={color}
            strokeWidth="2.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            points={points}
          />
        </svg>
        <div className="flex justify-between text-[10px] text-slate-500 font-mono mt-1">
          <span>Min: {min.toFixed(1)} {unit}</span>
          <span>Max: {max.toFixed(1)} {unit}</span>
        </div>
      </div>
    );
  };

  const throughputSeries = metrics.map(m => m.throughputOpsPerSec);
  const latencyP50Series = metrics.map(m => m.avgLatencyMs);
  const cacheHitSeries = metrics.map(m => m.cacheHitRatio * 100);
  const memorySeries = metrics.map(m => m.memoryUsedBytes / (1024 * 1024));

  return (
    <div className="flex-1 flex flex-col h-full overflow-y-auto p-6 space-y-6 bg-nexus-950">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-white tracking-tight">Performance Center</h1>
          <p className="text-xs text-slate-400 mt-1">Real-time throughput, latency percentiles, buffer hit ratio and storage telemetry</p>
        </div>
        <button
          onClick={fetchMetrics}
          disabled={loading}
          className="flex items-center gap-2 px-3 py-1.5 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 rounded-lg text-xs text-slate-300 transition"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          <span>Refresh</span>
        </button>
      </div>

      {/* Latency Percentiles Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4">
          <div className="text-xs text-slate-400 font-medium">Latency (p50)</div>
          <div className="text-2xl font-bold font-mono text-white mt-2">
            {(latest?.avgLatencyMs || 1.2).toFixed(2)} <span className="text-xs font-normal text-slate-400">ms</span>
          </div>
          <div className="text-[10px] text-slate-500 mt-1 font-mono">50th percentile query turnaround</div>
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4">
          <div className="text-xs text-slate-400 font-medium">Latency (p95)</div>
          <div className="text-2xl font-bold font-mono text-amber-300 mt-2">
            {(latest?.p95LatencyMs || 2.1).toFixed(2)} <span className="text-xs font-normal text-slate-400">ms</span>
          </div>
          <div className="text-[10px] text-slate-500 mt-1 font-mono">95th percentile query turnaround</div>
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4">
          <div className="text-xs text-slate-400 font-medium">Latency (p99)</div>
          <div className="text-2xl font-bold font-mono text-rose-400 mt-2">
            {(latest?.p99LatencyMs || 3.4).toFixed(2)} <span className="text-xs font-normal text-slate-400">ms</span>
          </div>
          <div className="text-[10px] text-slate-500 mt-1 font-mono">99th percentile tail latency</div>
        </div>
      </div>

      {/* Charts Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-5 shadow-lg">
          <div className="flex items-center gap-2 mb-3">
            <Zap className="w-4 h-4 text-amber-400" />
            <h2 className="text-xs font-bold uppercase tracking-wider text-slate-300">Throughput (ops/sec)</h2>
          </div>
          {renderLineChart(throughputSeries, '#f59e0b', 'ops/s', 'Execution Rate')}
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-5 shadow-lg">
          <div className="flex items-center gap-2 mb-3">
            <Clock className="w-4 h-4 text-sky-400" />
            <h2 className="text-xs font-bold uppercase tracking-wider text-slate-300">Average Turnaround (ms)</h2>
          </div>
          {renderLineChart(latencyP50Series, '#38bdf8', 'ms', 'p50 Latency')}
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-5 shadow-lg">
          <div className="flex items-center gap-2 mb-3">
            <Database className="w-4 h-4 text-emerald-400" />
            <h2 className="text-xs font-bold uppercase tracking-wider text-slate-300">Buffer Cache Hit Ratio (%)</h2>
          </div>
          {renderLineChart(cacheHitSeries, '#10b981', '%', 'Cache Hit Ratio')}
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-5 shadow-lg">
          <div className="flex items-center gap-2 mb-3">
            <HardDrive className="w-4 h-4 text-purple-400" />
            <h2 className="text-xs font-bold uppercase tracking-wider text-slate-300">Memory Utilization (MB)</h2>
          </div>
          {renderLineChart(memorySeries, '#c084fc', 'MB', 'JVM Heap In-Use')}
        </div>
      </div>
    </div>
  );
};
