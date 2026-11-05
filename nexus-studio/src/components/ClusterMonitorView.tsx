import React, { useEffect, useState } from 'react';
import { ApiClient, NodeInfo } from '../api/client';
import { Network, Crown, ArrowRight, RefreshCw, Radio } from 'lucide-react';

export const ClusterMonitorView: React.FC = () => {
  const [nodes, setNodes] = useState<NodeInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [wsConnected, setWsConnected] = useState(false);

  const fetchTopology = async () => {
    try {
      setLoading(true);
      const data = await ApiClient.getTopology();
      setNodes(data);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTopology();
    const interval = setInterval(fetchTopology, 3000);

    // Setup WebSocket connection to /ws/cluster
    let ws: WebSocket | null = null;
    try {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}/ws/cluster`;
      ws = new WebSocket(wsUrl);

      ws.onopen = () => setWsConnected(true);
      ws.onclose = () => setWsConnected(false);
      ws.onerror = () => setWsConnected(false);
      ws.onmessage = (event) => {
        try {
          const payload = JSON.parse(event.data);
          if (payload.topology) {
            setNodes(payload.topology);
          }
        } catch (ignored) {}
      };
    } catch (ignored) {
      setWsConnected(false);
    }

    return () => {
      clearInterval(interval);
      if (ws) ws.close();
    };
  }, []);

  const leader = nodes.find(n => n.role === 'LEADER');
  const followers = nodes.filter(n => n.role !== 'LEADER');

  return (
    <div className="flex-1 flex flex-col h-full overflow-y-auto p-6 space-y-6 bg-nexus-950">
      {/* Top Header */}
      <div className="flex items-center justify-between">
        <div>
          <div className="flex items-center gap-2.5">
            <h1 className="text-xl font-bold text-white tracking-tight">Cluster Topology Monitor</h1>
            <span className={`flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-[10px] font-mono font-bold tracking-wider uppercase border ${
              wsConnected
                ? 'bg-emerald-950 text-emerald-400 border-emerald-800'
                : 'bg-amber-950 text-amber-400 border-amber-800'
            }`}>
              <Radio className={`w-3 h-3 ${wsConnected ? 'animate-pulse' : ''}`} />
              {wsConnected ? 'WebSocket Live' : 'HTTP Polling'}
            </span>
          </div>
          <p className="text-xs text-slate-400 mt-1">Multi-Raft consensus group replication, terms and sync lag</p>
        </div>
        <button
          onClick={fetchTopology}
          disabled={loading}
          className="flex items-center gap-2 px-3 py-1.5 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 rounded-lg text-xs text-slate-300 transition"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          <span>Refresh</span>
        </button>
      </div>

      {/* Visual Consensus Graph */}
      <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-6 relative overflow-hidden shadow-xl">
        <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-6 flex items-center gap-2">
          <Network className="w-4 h-4 text-nexus-accent" />
          <span>Raft Quorum Replication Graph</span>
        </h2>

        <div className="flex flex-col lg:flex-row items-center justify-center gap-8 py-4">
          {/* Leader Node Card */}
          <div className="w-80 bg-gradient-to-b from-amber-950/40 via-nexus-900 to-nexus-850 border-2 border-amber-400/50 rounded-2xl p-5 shadow-2xl relative">
            <div className="absolute -top-3.5 left-1/2 -translate-x-1/2 px-3 py-0.5 bg-amber-400 text-nexus-950 rounded-full text-[11px] font-black uppercase tracking-wider flex items-center gap-1.5 shadow-md">
              <Crown className="w-3.5 h-3.5 fill-current" />
              <span>Raft Leader</span>
            </div>

            <div className="mt-2 text-center">
              <div className="text-lg font-bold font-mono text-white">{leader?.nodeId || 'node-1'}</div>
              <div className="text-xs font-mono text-amber-300/80">{leader?.address || '127.0.0.1:9091'}</div>
            </div>

            <div className="mt-4 pt-3 border-t border-nexus-800 space-y-2 text-xs">
              <div className="flex justify-between text-slate-400">
                <span>Current Term:</span>
                <span className="font-mono text-amber-300 font-bold">{leader?.term || 1}</span>
              </div>
              <div className="flex justify-between text-slate-400">
                <span>Commit Index:</span>
                <span className="font-mono text-slate-200">{leader?.commitIndex || 10}</span>
              </div>
              <div className="flex justify-between text-slate-400">
                <span>Buffer Hit Ratio:</span>
                <span className="font-mono text-emerald-400 font-bold">
                  {leader ? (leader.cacheHitRatio * 100).toFixed(1) : 98.5}%
                </span>
              </div>
              <div className="flex justify-between text-slate-400">
                <span>Active Transactions:</span>
                <span className="font-mono text-purple-400 font-bold">{leader?.activeTransactions || 0}</span>
              </div>
            </div>
          </div>

          <div className="hidden lg:flex flex-col items-center justify-center gap-2 text-slate-500 font-mono text-xs">
            <ArrowRight className="w-6 h-6 text-nexus-accent animate-pulse" />
            <span>AppendEntries</span>
          </div>

          {/* Follower Nodes Grid */}
          <div className="space-y-4 w-80">
            {followers.length === 0 ? (
              <div className="text-xs text-slate-500 text-center py-4">Searching for cluster peers...</div>
            ) : (
              followers.map((f) => (
                <div
                  key={f.nodeId}
                  className="bg-nexus-850 border border-nexus-800 hover:border-nexus-700 rounded-xl p-4 shadow-md transition"
                >
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      <span className="w-2 h-2 rounded-full bg-sky-400" />
                      <span className="font-mono font-bold text-xs text-white">{f.nodeId}</span>
                    </div>
                    <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-sky-950 text-sky-300 border border-sky-800 font-semibold">
                      FOLLOWER
                    </span>
                  </div>
                  <div className="space-y-1 text-[11px] text-slate-400">
                    <div className="flex justify-between">
                      <span>Endpoint:</span>
                      <span className="font-mono text-slate-300">{f.address}</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Replication Lag:</span>
                      <span className="font-mono text-emerald-400 font-semibold">{f.replicationLag} ms</span>
                    </div>
                    <div className="flex justify-between">
                      <span>Cache Hit:</span>
                      <span className="font-mono text-slate-300">{(f.cacheHitRatio * 100).toFixed(1)}%</span>
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>

      {/* Cluster Details Table */}
      <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-5">
        <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-4">Node Health Details</h2>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs font-mono">
            <thead>
              <tr className="border-b border-nexus-800 text-slate-400">
                <th className="pb-2">Node ID</th>
                <th className="pb-2">Role</th>
                <th className="pb-2">Address</th>
                <th className="pb-2">Status</th>
                <th className="pb-2">Cache Hit</th>
                <th className="pb-2">Active Txns</th>
                <th className="pb-2">Uptime</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-nexus-850">
              {nodes.map((n) => (
                <tr key={n.nodeId} className="hover:bg-nexus-850/50">
                  <td className="py-2.5 font-bold text-white flex items-center gap-1.5">
                    {n.role === 'LEADER' && <Crown className="w-3.5 h-3.5 text-amber-400" />}
                    <span>{n.nodeId}</span>
                  </td>
                  <td className="py-2.5">
                    <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                      n.role === 'LEADER' ? 'text-amber-300 bg-amber-950/50' : 'text-sky-300 bg-sky-950/50'
                    }`}>
                      {n.role}
                    </span>
                  </td>
                  <td className="py-2.5 text-slate-300">{n.address}</td>
                  <td className="py-2.5">
                    <span className="px-2 py-0.5 rounded text-[10px] bg-emerald-950 text-emerald-300 border border-emerald-800 font-bold">
                      {n.status}
                    </span>
                  </td>
                  <td className="py-2.5 text-emerald-400">{(n.cacheHitRatio * 100).toFixed(1)}%</td>
                  <td className="py-2.5 text-purple-300">{n.activeTransactions}</td>
                  <td className="py-2.5 text-slate-400">{Math.floor(n.uptimeSeconds / 60)}m {n.uptimeSeconds % 60}s</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};
