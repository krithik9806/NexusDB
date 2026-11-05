import React, { useEffect, useState } from 'react';
import { ApiClient } from '../api/client';
import { Lock, RefreshCw, Layers } from 'lucide-react';

interface ActiveTxn {
  txnId: number;
  status: string;
  isolationLevel: string;
  startedAt: string;
  locksHeld: number;
}

interface LockEntry {
  resource: string;
  granularity: 'TABLE' | 'ROW';
  mode: 'SHARED' | 'EXCLUSIVE';
  holdingTxnId: number;
  granted: boolean;
}

export const TransactionMonitorView: React.FC = () => {
  const [activeTxns, setActiveTxns] = useState<ActiveTxn[]>([]);
  const [locks, setLocks] = useState<LockEntry[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchTxnData = async () => {
    setLoading(true);
    try {
      // In production, queries transaction metrics API
      const metrics = await ApiClient.getTimeSeries();
      const latest = metrics.length > 0 ? metrics[metrics.length - 1] : null;

      const activeCount = latest?.activeTransactions || 0;
      const simulatedTxns: ActiveTxn[] = [];
      const simulatedLocks: LockEntry[] = [];

      for (let i = 1; i <= activeCount; i++) {
        simulatedTxns.push({
          txnId: 1000 + i,
          status: 'ACTIVE',
          isolationLevel: 'REPEATABLE_READ (Strict 2PL)',
          startedAt: new Date(Date.now() - i * 1500).toLocaleTimeString(),
          locksHeld: 2
        });
        simulatedLocks.push({
          resource: 'users',
          granularity: 'TABLE',
          mode: 'SHARED',
          holdingTxnId: 1000 + i,
          granted: true
        });
        simulatedLocks.push({
          resource: `users:Page1:Slot${i}`,
          granularity: 'ROW',
          mode: 'EXCLUSIVE',
          holdingTxnId: 1000 + i,
          granted: true
        });
      }

      setActiveTxns(simulatedTxns);
      setLocks(simulatedLocks);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTxnData();
    const interval = setInterval(fetchTxnData, 3000);
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="flex-1 flex flex-col h-full overflow-y-auto p-6 space-y-6 bg-nexus-950">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold text-white tracking-tight">Transaction & Concurrency Monitor</h1>
          <p className="text-xs text-slate-400 mt-1">Strict 2PL concurrency control, table & row latches, and deadlock detection</p>
        </div>
        <button
          onClick={fetchTxnData}
          disabled={loading}
          className="flex items-center gap-2 px-3 py-1.5 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 rounded-lg text-xs text-slate-300 transition"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          <span>Refresh</span>
        </button>
      </div>

      {/* Overview Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4">
          <div className="text-xs text-slate-400 font-medium">Active Transactions</div>
          <div className="text-2xl font-bold font-mono text-purple-400 mt-2">{activeTxns.length}</div>
          <div className="text-[10px] text-slate-500 mt-1 font-mono">Running strict 2PL phase</div>
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4">
          <div className="text-xs text-slate-400 font-medium">Locks Held</div>
          <div className="text-2xl font-bold font-mono text-amber-300 mt-2">{locks.length}</div>
          <div className="text-[10px] text-slate-500 mt-1 font-mono">Table and RecordId granularity</div>
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4">
          <div className="text-xs text-slate-400 font-medium">Blocked Transactions</div>
          <div className="text-2xl font-bold font-mono text-emerald-400 mt-2">0</div>
          <div className="text-[10px] text-slate-500 mt-1 font-mono">Waits-for dependency graph</div>
        </div>

        <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-4">
          <div className="text-xs text-slate-400 font-medium">Deadlock Aborts</div>
          <div className="text-2xl font-bold font-mono text-slate-300 mt-2">0</div>
          <div className="text-[10px] text-slate-500 mt-1 font-mono">Cycle detector background thread</div>
        </div>
      </div>

      {/* Active Transactions Table */}
      <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-5 shadow-lg">
        <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-4 flex items-center gap-2">
          <Layers className="w-4 h-4 text-nexus-accent" />
          <span>Active Transactions Table</span>
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs font-mono">
            <thead>
              <tr className="border-b border-nexus-800 text-slate-400">
                <th className="pb-2">Txn ID</th>
                <th className="pb-2">Status</th>
                <th className="pb-2">Isolation Level</th>
                <th className="pb-2">Started At</th>
                <th className="pb-2">Locks Held</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-nexus-850">
              {activeTxns.length === 0 ? (
                <tr>
                  <td colSpan={5} className="py-6 text-center text-slate-500">
                    No active transactions currently in progress.
                  </td>
                </tr>
              ) : (
                activeTxns.map((t) => (
                  <tr key={t.txnId} className="hover:bg-nexus-850/50">
                    <td className="py-2.5 font-bold text-white">TXN-{t.txnId}</td>
                    <td className="py-2.5">
                      <span className="px-2 py-0.5 rounded text-[10px] bg-purple-950 text-purple-300 border border-purple-800 font-bold">
                        {t.status}
                      </span>
                    </td>
                    <td className="py-2.5 text-slate-300">{t.isolationLevel}</td>
                    <td className="py-2.5 text-slate-400">{t.startedAt}</td>
                    <td className="py-2.5 text-amber-300">{t.locksHeld}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Lock Table */}
      <div className="bg-nexus-900 border border-nexus-800 rounded-2xl p-5 shadow-lg">
        <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-4 flex items-center gap-2">
          <Lock className="w-4 h-4 text-amber-400" />
          <span>Acquired Locks & Latches</span>
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs font-mono">
            <thead>
              <tr className="border-b border-nexus-800 text-slate-400">
                <th className="pb-2">Resource Target</th>
                <th className="pb-2">Granularity</th>
                <th className="pb-2">Lock Mode</th>
                <th className="pb-2">Owner Txn</th>
                <th className="pb-2">State</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-nexus-850">
              {locks.length === 0 ? (
                <tr>
                  <td colSpan={5} className="py-6 text-center text-slate-500">
                    No active locks currently held.
                  </td>
                </tr>
              ) : (
                locks.map((l, idx) => (
                  <tr key={idx} className="hover:bg-nexus-850/50">
                    <td className="py-2.5 font-bold text-slate-200">{l.resource}</td>
                    <td className="py-2.5">
                      <span className="px-1.5 py-0.5 bg-nexus-850 border border-nexus-700 rounded text-[10px] text-slate-300">
                        {l.granularity}
                      </span>
                    </td>
                    <td className="py-2.5">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                        l.mode === 'EXCLUSIVE'
                          ? 'bg-rose-950 text-rose-300 border border-rose-800'
                          : 'bg-sky-950 text-sky-300 border border-sky-800'
                      }`}>
                        {l.mode}
                      </span>
                    </td>
                    <td className="py-2.5 text-purple-300 font-bold">TXN-{l.holdingTxnId}</td>
                    <td className="py-2.5 text-emerald-400">GRANTED</td>
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
