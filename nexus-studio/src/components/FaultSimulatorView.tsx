import React, { useEffect, useState } from 'react';
import { ApiClient, NodeInfo } from '../api/client';
import { AlertTriangle, ShieldAlert, WifiOff, Clock, RotateCcw, Zap, RefreshCw, CheckCircle2 } from 'lucide-react';

interface ConfirmModalProps {
  isOpen: boolean;
  title: string;
  message: string;
  onConfirm: () => void;
  onCancel: () => void;
  loading: boolean;
}

const ConfirmModal: React.FC<ConfirmModalProps> = ({ isOpen, title, message, onConfirm, onCancel, loading }) => {
  if (!isOpen) return null;
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm p-4">
      <div className="bg-nexus-900 border border-red-800 rounded-2xl p-6 max-w-md w-full shadow-2xl space-y-4">
        <div className="flex items-center gap-3 text-red-400">
          <AlertTriangle className="w-6 h-6 shrink-0" />
          <h3 className="text-base font-bold text-white">{title}</h3>
        </div>
        <p className="text-xs text-slate-300 leading-relaxed font-mono">{message}</p>
        <div className="flex justify-end gap-3 pt-2">
          <button
            onClick={onCancel}
            disabled={loading}
            className="px-4 py-2 bg-nexus-800 hover:bg-nexus-700 text-slate-300 rounded-lg text-xs font-semibold transition"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={loading}
            className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg text-xs font-bold transition shadow-lg shadow-red-900/30"
          >
            {loading ? 'Executing...' : 'Confirm Injection'}
          </button>
        </div>
      </div>
    </div>
  );
};

export const FaultSimulatorView: React.FC = () => {
  const [nodes, setNodes] = useState<NodeInfo[]>([]);
  const [auditLogs, setAuditLogs] = useState<any[]>([]);
  const [selectedNode, setSelectedNode] = useState<string>('node-2');
  const [pendingAction, setPendingAction] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<{ text: string; isError?: boolean } | null>(null);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);

  const loadData = async () => {
    try {
      setRefreshing(true);
      const [topo, logs] = await Promise.all([
        ApiClient.getTopology(),
        ApiClient.getAuditLog(15).catch(() => [])
      ]);
      setNodes(topo);
      setAuditLogs(logs);
      if (topo.length > 0 && !topo.find(n => n.nodeId === selectedNode)) {
        setSelectedNode(topo[0].nodeId);
      }
    } finally {
      setRefreshing(false);
    }
  };

  useEffect(() => {
    loadData();
  }, []);

  const handleActionClick = (action: string) => {
    setPendingAction(action);
  };

  const handleConfirmAction = async () => {
    if (!pendingAction) return;
    setLoading(true);
    setStatusMessage(null);
    try {
      await ApiClient.injectFault(selectedNode, pendingAction, 3000);
      setStatusMessage({ text: `Successfully injected ${pendingAction} on ${selectedNode}!` });
      setPendingAction(null);
      await loadData();
    } catch (err: any) {
      setStatusMessage({ text: `Failed to inject ${pendingAction}: ${err.message}`, isError: true });
      setPendingAction(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex-1 flex flex-col h-full overflow-y-auto p-6 space-y-6 bg-nexus-950">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <div className="flex items-center gap-2">
            <ShieldAlert className="w-5 h-5 text-red-400" />
            <h1 className="text-xl font-bold text-white tracking-tight">Chaos & Fault Simulator</h1>
            <span className="px-2 py-0.5 bg-red-950 text-red-300 border border-red-800 rounded text-[10px] font-bold">
              ADMIN ONLY
            </span>
          </div>
          <p className="text-xs text-slate-400 mt-1">Inject network partitions, latency and leader elections into live cluster nodes</p>
        </div>
        <button
          onClick={loadData}
          disabled={refreshing}
          className="flex items-center gap-2 px-3 py-1.5 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 rounded-lg text-xs text-slate-300 transition"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`} />
          <span>Refresh</span>
        </button>
      </div>

      {statusMessage && (
        <div className={`p-4 rounded-xl text-xs flex items-center gap-2.5 shadow-lg border ${
          statusMessage.isError
            ? 'bg-red-950/60 border-red-800 text-red-200'
            : 'bg-emerald-950/60 border-emerald-800 text-emerald-200'
        }`}>
          {statusMessage.isError ? <AlertTriangle className="w-4 h-4 shrink-0" /> : <CheckCircle2 className="w-4 h-4 shrink-0" />}
          <span className="font-mono">{statusMessage.text}</span>
        </div>
      )}

      {/* Control Panel */}
      <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-6 shadow-xl space-y-6">
        <div>
          <label className="block text-xs font-bold uppercase tracking-wider text-slate-400 mb-2">
            Select Target Cluster Node
          </label>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {nodes.map((n) => (
              <button
                key={n.nodeId}
                onClick={() => setSelectedNode(n.nodeId)}
                className={`p-3 rounded-xl border text-left transition flex items-center justify-between ${
                  selectedNode === n.nodeId
                    ? 'bg-nexus-800 border-nexus-accent text-white shadow-md'
                    : 'bg-nexus-850 border-nexus-800 text-slate-300 hover:border-nexus-700'
                }`}
              >
                <div>
                  <div className="font-mono font-bold text-xs">{n.nodeId}</div>
                  <div className="text-[11px] text-slate-400 font-mono mt-0.5">{n.address}</div>
                </div>
                <span className={`text-[10px] font-mono px-2 py-0.5 rounded font-bold uppercase ${
                  n.role === 'LEADER' ? 'bg-amber-950 text-amber-300' : 'bg-slate-700 text-slate-300'
                }`}>
                  {n.role}
                </span>
              </button>
            ))}
          </div>
        </div>

        {/* Fault Action Cards */}
        <div>
          <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3">
            Available Fault Actions for {selectedNode}
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <button
              onClick={() => handleActionClick('PARTITION')}
              className="p-4 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 hover:border-amber-400/50 rounded-xl text-left transition space-y-2 group"
            >
              <div className="p-2 bg-amber-400/10 text-amber-400 rounded-lg w-fit group-hover:scale-110 transition">
                <WifiOff className="w-5 h-5" />
              </div>
              <div className="font-bold text-xs text-white">Partition Traffic</div>
              <p className="text-[11px] text-slate-400 leading-relaxed">
                Drop all incoming and outgoing Raft RPC messages to simulate network partition.
              </p>
            </button>

            <button
              onClick={() => handleActionClick('LATENCY')}
              className="p-4 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 hover:border-sky-400/50 rounded-xl text-left transition space-y-2 group"
            >
              <div className="p-2 bg-sky-400/10 text-sky-400 rounded-lg w-fit group-hover:scale-110 transition">
                <Clock className="w-5 h-5" />
              </div>
              <div className="font-bold text-xs text-white">Inject Network Delay</div>
              <p className="text-[11px] text-slate-400 leading-relaxed">
                Add 500ms artificial network latency on all message deliveries to test heartbeats.
              </p>
            </button>

            <button
              onClick={() => handleActionClick('FORCE_ELECTION')}
              className="p-4 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 hover:border-purple-400/50 rounded-xl text-left transition space-y-2 group"
            >
              <div className="p-2 bg-purple-400/10 text-purple-400 rounded-lg w-fit group-hover:scale-110 transition">
                <Zap className="w-5 h-5" />
              </div>
              <div className="font-bold text-xs text-white">Force Election</div>
              <p className="text-[11px] text-slate-400 leading-relaxed">
                Trigger an immediate randomized election timeout and candidate term increment.
              </p>
            </button>

            <button
              onClick={() => handleActionClick('HEAL')}
              className="p-4 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 hover:border-emerald-400/50 rounded-xl text-left transition space-y-2 group"
            >
              <div className="p-2 bg-emerald-400/10 text-emerald-400 rounded-lg w-fit group-hover:scale-110 transition">
                <RotateCcw className="w-5 h-5" />
              </div>
              <div className="font-bold text-xs text-white">Heal / Clear Faults</div>
              <p className="text-[11px] text-slate-400 leading-relaxed">
                Remove all active fault injections and latency modifiers to restore normal communication.
              </p>
            </button>
          </div>
        </div>
      </div>

      {/* Audit Log Table */}
      <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-5 shadow-lg">
        <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-4">
          Control Plane Security Audit Log
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs font-mono">
            <thead>
              <tr className="border-b border-nexus-800 text-slate-400">
                <th className="pb-2">User</th>
                <th className="pb-2">Action</th>
                <th className="pb-2">Target Node</th>
                <th className="pb-2">Details</th>
                <th className="pb-2">Timestamp</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-nexus-850">
              {auditLogs.length === 0 ? (
                <tr>
                  <td colSpan={5} className="py-6 text-center text-slate-500">
                    No administrative fault actions executed yet.
                  </td>
                </tr>
              ) : (
                auditLogs.map((log, idx) => (
                  <tr key={idx} className="hover:bg-nexus-850/50">
                    <td className="py-2.5 font-bold text-purple-300">{log.username}</td>
                    <td className="py-2.5 text-white font-bold">{log.action}</td>
                    <td className="py-2.5 text-amber-300">{log.target_node}</td>
                    <td className="py-2.5 text-slate-300 truncate max-w-xs">{log.details}</td>
                    <td className="py-2.5 text-slate-400 text-[11px]">
                      {new Date(log.created_at).toLocaleString()}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Confirmation Dialog */}
      <ConfirmModal
        isOpen={!!pendingAction}
        title={`Confirm ${pendingAction} on ${selectedNode}`}
        message={`Are you sure you want to execute ${pendingAction} on ${selectedNode}? This may induce simulated cluster instability, leader step-downs, or log divergence until healed.`}
        onConfirm={handleConfirmAction}
        onCancel={() => setPendingAction(null)}
        loading={loading}
      />
    </div>
  );
};
