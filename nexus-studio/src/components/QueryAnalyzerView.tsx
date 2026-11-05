import React, { useState } from 'react';
import { ApiClient } from '../api/client';
import { GitGraph, Play, AlertCircle, FileText, Network, CheckCircle2 } from 'lucide-react';

export interface PlanNode {
  id: string;
  depth: number;
  operator: string;
  details: string;
  cost?: string;
  rows?: string;
}

export function parseExplainOutput(text: string): PlanNode[] {
  const lines = text.split('\n').filter(l => l.trim().length > 0);
  return lines.map((line, idx) => {
    // Calculate indentation
    const trimmed = line.trimStart();
    const leadingSpaces = line.length - trimmed.length;
    const depth = Math.floor(leadingSpaces / 2);

    // Extract operator and details (e.g., "-> SeqScan on users (cost=12.50 rows=100)")
    let clean = trimmed;
    if (clean.startsWith('->') || clean.startsWith('-') || clean.startsWith('*')) {
      clean = clean.replace(/^[-*>|\s]+/, '').trim();
    }

    const costMatch = clean.match(/cost=([\d.]+)/i);
    const rowsMatch = clean.match(/rows=(\d+)/i);

    const operator = clean.split(/[\s(]/)[0] || 'Operator';
    const details = clean;

    return {
      id: `node-${idx}`,
      depth,
      operator,
      details,
      cost: costMatch ? costMatch[1] : undefined,
      rows: rowsMatch ? rowsMatch[1] : undefined
    };
  });
}

export const QueryAnalyzerView: React.FC = () => {
  const [sql, setSql] = useState('SELECT * FROM users WHERE id = 1;');
  const [rawPlan, setRawPlan] = useState<string | null>(null);
  const [parsedNodes, setParsedNodes] = useState<PlanNode[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [viewMode, setViewMode] = useState<'visual' | 'raw'>('visual');

  const handleAnalyze = async () => {
    if (!sql.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const res = await ApiClient.explainSql(sql.trim());
      setRawPlan(res.explanation);
      const nodes = parseExplainOutput(res.explanation);
      setParsedNodes(nodes);
    } catch (err: any) {
      setError(err.message || 'Analysis failed');
      setRawPlan(null);
      setParsedNodes([]);
    } finally {
      setLoading(false);
    }
  };

  const getOperatorColor = (op: string) => {
    const upper = op.toUpperCase();
    if (upper.includes('SCAN')) return 'bg-sky-950/80 text-sky-300 border-sky-800';
    if (upper.includes('JOIN')) return 'bg-purple-950/80 text-purple-300 border-purple-800';
    if (upper.includes('FILTER')) return 'bg-emerald-950/80 text-emerald-300 border-emerald-800';
    if (upper.includes('SORT') || upper.includes('LIMIT')) return 'bg-amber-950/80 text-amber-300 border-amber-800';
    if (upper.includes('AGGREGATE') || upper.includes('GROUP')) return 'bg-rose-950/80 text-rose-300 border-rose-800';
    return 'bg-nexus-850 text-slate-300 border-nexus-700';
  };

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden bg-nexus-950">
      {/* Top Query Input Bar */}
      <div className="p-5 border-b border-nexus-800 bg-nexus-900 space-y-3">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-sm font-bold text-white tracking-wide">Query Plan Analyzer</h1>
            <p className="text-[11px] text-slate-400">Inspect cost-based volcano execution plan trees, selectivity and cost estimations</p>
          </div>
          <button
            onClick={handleAnalyze}
            disabled={loading}
            className="px-4 py-1.5 bg-nexus-accent hover:bg-nexus-accentDark text-nexus-950 font-bold rounded-lg text-xs flex items-center gap-1.5 transition shadow-sm"
          >
            <Play className="w-3.5 h-3.5 fill-current" />
            <span>{loading ? 'Analyzing...' : 'Generate Plan'}</span>
          </button>
        </div>

        <input
          type="text"
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleAnalyze()}
          placeholder="Enter SQL to analyze (e.g. SELECT * FROM users WHERE id = 1;)"
          className="w-full bg-nexus-950 border border-nexus-800 rounded-lg px-3 py-2 text-xs font-mono text-slate-100 placeholder-slate-500 focus:outline-none focus:border-nexus-accent focus:ring-1 focus:ring-nexus-accent"
        />
      </div>

      {/* Plan Toolbar & Mode Switcher */}
      <div className="h-9 border-b border-nexus-800 bg-nexus-900 px-5 flex items-center justify-between">
        <div className="flex items-center gap-4 text-xs font-medium">
          <button
            onClick={() => setViewMode('visual')}
            className={`h-9 flex items-center gap-1.5 border-b-2 transition ${
              viewMode === 'visual'
                ? 'border-nexus-accent text-nexus-accent'
                : 'border-transparent text-slate-400 hover:text-slate-200'
            }`}
          >
            <Network className="w-3.5 h-3.5" />
            <span>Visual Tree Plan</span>
          </button>
          <button
            onClick={() => setViewMode('raw')}
            className={`h-9 flex items-center gap-1.5 border-b-2 transition ${
              viewMode === 'raw'
                ? 'border-nexus-accent text-nexus-accent'
                : 'border-transparent text-slate-400 hover:text-slate-200'
            }`}
          >
            <FileText className="w-3.5 h-3.5" />
            <span>Raw EXPLAIN Text</span>
          </button>
        </div>

        {parsedNodes.length > 0 && (
          <div className="flex items-center gap-2 text-[11px] text-slate-400 font-mono">
            <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" />
            <span>{parsedNodes.length} Plan Operators</span>
          </div>
        )}
      </div>

      {/* Main Content Area */}
      <div className="flex-1 overflow-auto p-6">
        {error && (
          <div className="p-4 bg-red-950/60 border border-red-800 rounded-xl text-red-200 text-xs flex items-start gap-3">
            <AlertCircle className="w-5 h-5 text-red-400 shrink-0 mt-0.5" />
            <div>
              <div className="font-bold mb-1">Planning Error</div>
              <div className="font-mono text-red-300">{error}</div>
            </div>
          </div>
        )}

        {!error && rawPlan && viewMode === 'visual' && (
          <div className="max-w-3xl mx-auto space-y-3">
            {parsedNodes.map((node) => (
              <div
                key={node.id}
                style={{ marginLeft: `${node.depth * 28}px` }}
                className={`p-3.5 rounded-xl border transition shadow-sm ${getOperatorColor(node.operator)}`}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-2">
                    <span className="font-mono font-bold text-xs">{node.operator}</span>
                    <span className="text-[11px] text-slate-300 font-mono opacity-90 truncate max-w-md">
                      {node.details}
                    </span>
                  </div>
                  <div className="flex items-center gap-2 text-[10px] font-mono shrink-0">
                    {node.cost && (
                      <span className="px-2 py-0.5 bg-black/30 rounded border border-white/10">
                        cost={node.cost}
                      </span>
                    )}
                    {node.rows && (
                      <span className="px-2 py-0.5 bg-black/30 rounded border border-white/10">
                        rows={node.rows}
                      </span>
                    )}
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}

        {!error && rawPlan && viewMode === 'raw' && (
          <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-5 font-mono text-xs text-slate-200 whitespace-pre leading-relaxed overflow-x-auto shadow-inner">
            {rawPlan}
          </div>
        )}

        {!error && !rawPlan && (
          <div className="h-full flex flex-col items-center justify-center text-slate-500 text-xs">
            <GitGraph className="w-8 h-8 text-slate-600 mb-2" />
            <span>Enter a query above and click Generate Plan to analyze execution costs and scan paths.</span>
          </div>
        )}
      </div>
    </div>
  );
};
