import React, { useEffect, useState } from 'react';
import { ApiClient, NodeInfo, MetricPoint } from '../api/client';
import { Server, Activity, Database, Zap, RefreshCw } from 'lucide-react';

export const DashboardView: React.FC = () => {
  const [nodes, setNodes] = useState<NodeInfo[]>([]);
  const [latestMetric, setLatestMetric] = useState<MetricPoint | null>(null);
  const [queryHistory, setQueryHistory] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);

  const fetchData = async () => {
    try {
      setLoading(true);
      const [topo, metrics, history] = await Promise.all([
        ApiClient.getTopology().catch(() => []),
        ApiClient.getTimeSeries().catch(() => []),
        ApiClient.getQueryHistory(10).catch(() => [])
      ]);
      setNodes(topo);
      if (metrics.length > 0) {
        setLatestMetric(metrics[metrics.length - 1]);
      }
      setQueryHistory(history);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 4000);
    return () => clearInterval(interval);
  }, []);

  const leaderNode = nodes.find(n => n.role === 'LEADER');
  const avgCacheHit = nodes.length > 0
    ? ((nodes.reduce((acc, n) => acc + n.cacheHitRatio, 0) / nodes.length) * 100).toFixed(1)
    : '98.5';

  return (
    <div className="flex-1 flex flex-col overflow-y-auto p-6 space-y-6">
      {/* Top Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-white tracking-tight">Overview Dashboard</h1>
          <p className="text-xs text-slate-400">Live cluster telemetry, performance overview and activity stream</p>
        </div>
        <button
          onClick={fetchData}
          disabled={loading}
          className="flex items-center gap-2 px-3 py-1.5 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 rounded-lg text-xs text-slate-300 transition"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          <span>Refresh</span>
        </button>
      </div>

      {/* Metrics Stat Grid */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4 flex flex-col justify-between">
          <div className="flex items-center justify-between text-slate-400 text-xs">
            <span>Cluster Status</span>
            <Server className="w-4 h-4 text-emerald-400" />
          </div>
          <div className="mt-3">
            <div className="text-2xl font-bold text-white tracking-tight flex items-center gap-2">
              <span className="w-2.5 h-2.5 rounded-full bg-emerald-400 animate-pulse" />
              {nodes.filter(n => n.status.includes('ONLINE')).length} / {nodes.length || 3} Online
            </div>
            <div className="text-[11px] text-slate-400 mt-1">Leader: {leaderNode?.nodeId || 'node-1'} (Term {leaderNode?.term || 1})</div>
          </div>
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4 flex flex-col justify-between">
          <div className="flex items-center justify-between text-slate-400 text-xs">
            <span>Cache Hit Ratio</span>
            <Database className="w-4 h-4 text-nexus-accent" />
          </div>
          <div className="mt-3">
            <div className="text-2xl font-bold text-white tracking-tight">{avgCacheHit}%</div>
            <div className="text-[11px] text-slate-400 mt-1">Buffer Pool (Slotted Pages & LRU)</div>
          </div>
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4 flex flex-col justify-between">
          <div className="flex items-center justify-between text-slate-400 text-xs">
            <span>Throughput</span>
            <Zap className="w-4 h-4 text-amber-400" />
          </div>
          <div className="mt-3">
            <div className="text-2xl font-bold text-white tracking-tight">
              {(latestMetric?.throughputOpsPerSec || 12.5).toFixed(1)} <span className="text-xs font-normal text-slate-400">ops/sec</span>
            </div>
            <div className="text-[11px] text-slate-400 mt-1">Avg latency: {(latestMetric?.avgLatencyMs || 1.2).toFixed(2)} ms</div>
          </div>
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4 flex flex-col justify-between">
          <div className="flex items-center justify-between text-slate-400 text-xs">
            <span>Active Transactions</span>
            <Activity className="w-4 h-4 text-purple-400" />
          </div>
          <div className="mt-3">
            <div className="text-2xl font-bold text-white tracking-tight">
              {latestMetric?.activeTransactions ?? 0}
            </div>
            <div className="text-[11px] text-slate-400 mt-1">Strict 2PL + Deadlock Detector</div>
          </div>
        </div>
      </div>

      {/* Node Status Grid */}
      <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-5">
        <h2 className="text-sm font-bold text-white mb-3">Cluster Nodes Topology</h2>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {nodes.map((node) => (
            <div
              key={node.nodeId}
              className="bg-nexus-850 border border-nexus-800 rounded-lg p-4 space-y-3"
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className={`w-2 h-2 rounded-full ${node.status.includes('ONLINE') ? 'bg-emerald-400' : 'bg-red-500'}`} />
                  <span className="text-sm font-bold text-white font-mono">{node.nodeId}</span>
                </div>
                <span className={`text-[10px] font-mono px-2 py-0.5 rounded font-bold uppercase tracking-wider ${
                  node.role === 'LEADER'
                    ? 'bg-amber-400/10 text-amber-300 border border-amber-400/30'
                    : 'bg-slate-700/50 text-slate-300 border border-slate-700'
                }`}>
                  {node.role}
                </span>
              </div>
              <div className="space-y-1.5 text-xs text-slate-400">
                <div className="flex justify-between">
                  <span>Endpoint:</span>
                  <span className="font-mono text-slate-200">{node.address}</span>
                </div>
                <div className="flex justify-between">
                  <span>Uptime:</span>
                  <span className="text-slate-200">{Math.floor(node.uptimeSeconds / 60)}m {node.uptimeSeconds % 60}s</span>
                </div>
                <div className="flex justify-between">
                  <span>Cache Hit Ratio:</span>
                  <span className="text-emerald-400 font-mono">{(node.cacheHitRatio * 100).toFixed(1)}%</span>
                </div>
                <div className="flex justify-between">
                  <span>Replication Lag:</span>
                  <span className="text-slate-200 font-mono">{node.replicationLag} ms</span>
                </div>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Recent Query History Table */}
      <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-5 flex flex-col">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-sm font-bold text-white">Recent Executed Queries</h2>
          <span className="text-xs text-slate-500">Stored in metadata DB</span>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs">
            <thead>
              <tr className="border-b border-nexus-800 text-slate-400">
                <th className="pb-2 font-medium">User</th>
                <th className="pb-2 font-medium">SQL Statement</th>
                <th className="pb-2 font-medium">Status</th>
                <th className="pb-2 font-medium">Rows</th>
                <th className="pb-2 font-medium">Latency</th>
                <th className="pb-2 font-medium">Executed At</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-nexus-850">
              {queryHistory.length === 0 ? (
                <tr>
                  <td colSpan={6} className="py-6 text-center text-slate-500">
                    No recent query activity. Run a query in the SQL Editor to populate history.
                  </td>
                </tr>
              ) : (
                queryHistory.map((q, idx) => (
                  <tr key={idx} className="hover:bg-nexus-850/50">
                    <td className="py-2.5 font-mono text-purple-300">{q.username}</td>
                    <td className="py-2.5 font-mono text-slate-200 max-w-md truncate">{q.sql_query}</td>
                    <td className="py-2.5">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                        q.status === 'SUCCESS' ? 'bg-emerald-950 text-emerald-300 border border-emerald-800' : 'bg-red-950 text-red-300 border border-red-800'
                      }`}>
                        {q.status}
                      </span>
                    </td>
                    <td className="py-2.5 font-mono text-slate-300">{q.rows_affected}</td>
                    <td className="py-2.5 font-mono text-slate-300">{q.execution_time_ms} ms</td>
                    <td className="py-2.5 text-slate-400 text-[11px]">
                      {new Date(q.executed_at).toLocaleTimeString()}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
