import React, { useState, useEffect, useRef } from 'react';
import { ApiClient, SqlResult } from '../api/client';
import { Play, RotateCcw, Clock, CheckCircle2, AlertCircle, History, Sparkles } from 'lucide-react';

export const SqlEditorView: React.FC = () => {
  const [sql, setSql] = useState('SELECT * FROM users;');
  const [result, setResult] = useState<SqlResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState<any[]>([]);
  const [activeTab, setActiveTab] = useState<'results' | 'history'>('results');
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  const loadHistory = async () => {
    try {
      const hist = await ApiClient.getQueryHistory(20);
      setHistory(hist);
    } catch (ignored) {}
  };

  useEffect(() => {
    loadHistory();
  }, []);

  const handleExecute = async () => {
    if (!sql.trim()) return;
    setLoading(true);
    setError(null);
    try {
      const res = await ApiClient.executeSql(sql.trim());
      setResult(res);
      setActiveTab('results');
      loadHistory();
    } catch (err: any) {
      setError(err.message || 'Execution failed');
      setResult(null);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // Ctrl+Enter or Cmd+Enter to execute
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      handleExecute();
    }
    // Tab key inserts 2 spaces
    if (e.key === 'Tab') {
      e.preventDefault();
      const target = e.currentTarget;
      const start = target.selectionStart;
      const end = target.selectionEnd;
      const newSql = sql.substring(0, start) + '  ' + sql.substring(end);
      setSql(newSql);
      setTimeout(() => {
        target.selectionStart = target.selectionEnd = start + 2;
      }, 0);
    }
  };

  const sampleQueries = [
    'SELECT * FROM users;',
    'INSERT INTO users VALUES (3, \'carol\', true);',
    'SELECT COUNT(*), active FROM users GROUP BY active;',
    'EXPLAIN SELECT * FROM users WHERE id = 1;'
  ];

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden bg-nexus-950">
      {/* Editor Panel */}
      <div className="flex flex-col border-b border-nexus-800 bg-nexus-900">
        <div className="h-10 border-b border-nexus-800 px-4 flex items-center justify-between bg-nexus-850">
          <div className="flex items-center gap-2">
            <span className="text-xs font-semibold text-slate-300">SQL Console</span>
            <span className="text-[10px] text-slate-500 font-mono">(Press Ctrl + Enter to run)</span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setSql('')}
              className="p-1 text-slate-400 hover:text-slate-200 hover:bg-nexus-800 rounded text-xs flex items-center gap-1"
              title="Clear editor"
            >
              <RotateCcw className="w-3.5 h-3.5" />
            </button>
            <button
              onClick={handleExecute}
              disabled={loading}
              className="px-3 py-1 bg-nexus-accent hover:bg-nexus-accentDark text-nexus-950 font-bold rounded text-xs flex items-center gap-1.5 transition shadow-sm"
            >
              <Play className="w-3.5 h-3.5 fill-current" />
              <span>{loading ? 'Executing...' : 'Run Query'}</span>
            </button>
          </div>
        </div>

        {/* Text Area Editor */}
        <div className="relative p-2 font-mono text-xs">
          <textarea
            ref={textareaRef}
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            onKeyDown={handleKeyDown}
            rows={6}
            placeholder="Write SQL statements here (e.g. SELECT * FROM users;)..."
            className="w-full bg-nexus-950 border border-nexus-800 rounded-lg p-3 text-slate-100 placeholder-slate-600 focus:outline-none focus:border-nexus-accent focus:ring-1 focus:ring-nexus-accent resize-none font-mono text-sm leading-relaxed"
          />
          <div className="flex items-center gap-2 mt-2 px-1 text-[11px] text-slate-400">
            <Sparkles className="w-3.5 h-3.5 text-amber-400 shrink-0" />
            <span className="shrink-0 font-medium">Quick Templates:</span>
            <div className="flex flex-wrap gap-1.5">
              {sampleQueries.map((q, idx) => (
                <button
                  key={idx}
                  onClick={() => setSql(q)}
                  className="px-2 py-0.5 bg-nexus-850 hover:bg-nexus-800 border border-nexus-700 rounded text-[10px] text-slate-300 font-mono transition"
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        </div>
      </div>

      {/* Results / History Navigation Tabs */}
      <div className="h-9 border-b border-nexus-800 bg-nexus-900 px-4 flex items-center justify-between">
        <div className="flex items-center gap-4 text-xs font-medium">
          <button
            onClick={() => setActiveTab('results')}
            className={`h-9 flex items-center gap-1.5 border-b-2 transition ${
              activeTab === 'results'
                ? 'border-nexus-accent text-nexus-accent'
                : 'border-transparent text-slate-400 hover:text-slate-200'
            }`}
          >
            <span>Result Set</span>
            {result && (
              <span className="text-[10px] px-1.5 py-0.2 bg-nexus-800 text-slate-300 rounded-full font-mono">
                {result.rows.length} rows
              </span>
            )}
          </button>
          <button
            onClick={() => setActiveTab('history')}
            className={`h-9 flex items-center gap-1.5 border-b-2 transition ${
              activeTab === 'history'
                ? 'border-nexus-accent text-nexus-accent'
                : 'border-transparent text-slate-400 hover:text-slate-200'
            }`}
          >
            <History className="w-3.5 h-3.5" />
            <span>Query History</span>
          </button>
        </div>

        {/* Execution Metadata Badge */}
        {result && (
          <div className="flex items-center gap-3 text-[11px] text-slate-400 font-mono">
            <div className="flex items-center gap-1">
              <Clock className="w-3.5 h-3.5 text-slate-400" />
              <span>{result.executionTimeMs} ms</span>
            </div>
            {result.rowsAffected > 0 && (
              <div className="flex items-center gap-1 text-emerald-400">
                <CheckCircle2 className="w-3.5 h-3.5" />
                <span>{result.rowsAffected} affected</span>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Output Content Area */}
      <div className="flex-1 overflow-auto p-4">
        {error && (
          <div className="p-4 bg-red-950/60 border border-red-800 rounded-xl text-red-200 text-xs flex items-start gap-3 shadow-lg">
            <AlertCircle className="w-5 h-5 text-red-400 shrink-0 mt-0.5" />
            <div>
              <div className="font-bold mb-1">Execution Error</div>
              <div className="font-mono text-red-300 leading-relaxed whitespace-pre-wrap">{error}</div>
            </div>
          </div>
        )}

        {activeTab === 'results' && !error && (
          result ? (
            result.columns.length > 0 ? (
              <div className="border border-nexus-800 rounded-lg overflow-hidden bg-nexus-900 shadow-md">
                <table className="w-full text-left text-xs border-collapse">
                  <thead>
                    <tr className="bg-nexus-850 border-b border-nexus-800">
                      {result.columns.map((col, idx) => (
                        <th key={idx} className="py-2.5 px-4 font-mono font-semibold text-slate-300">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-nexus-850">
                    {result.rows.length === 0 ? (
                      <tr>
                        <td colSpan={result.columns.length} className="py-6 text-center text-slate-500 font-mono">
                          0 rows returned
                        </td>
                      </tr>
                    ) : (
                      result.rows.map((row, rIdx) => (
                        <tr key={rIdx} className="hover:bg-nexus-850/60 font-mono text-slate-200 transition">
                          {row.map((val, cIdx) => (
                            <td key={cIdx} className="py-2 px-4 whitespace-nowrap">
                              {val === 'null' || val === null ? (
                                <span className="text-slate-500 italic">NULL</span>
                              ) : (
                                String(val)
                              )}
                            </td>
                          ))}
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            ) : (
              <div className="p-6 bg-nexus-900 border border-nexus-800 rounded-xl text-center">
                <CheckCircle2 className="w-8 h-8 text-emerald-400 mx-auto mb-2" />
                <div className="text-sm font-bold text-white">Statement Executed Successfully</div>
                <div className="text-xs text-slate-400 mt-1">
                  Rows affected: {result.rowsAffected} | Time: {result.executionTimeMs} ms
                </div>
              </div>
            )
          ) : (
            <div className="h-full flex flex-col items-center justify-center text-slate-500 text-xs">
              <Play className="w-8 h-8 text-slate-600 mb-2" />
              <span>Write a SQL query and click Run Query (or press Ctrl+Enter) to view results.</span>
            </div>
          )
        )}

        {activeTab === 'history' && (
          <div className="space-y-2">
            {history.length === 0 ? (
              <div className="text-center py-8 text-slate-500 text-xs">No execution history recorded.</div>
            ) : (
              history.map((h, idx) => (
                <div
                  key={idx}
                  onClick={() => setSql(h.sql_query)}
                  className="p-3 bg-nexus-900 border border-nexus-800 hover:border-nexus-700 rounded-lg cursor-pointer transition flex items-center justify-between"
                >
                  <div className="flex-1 pr-4">
                    <div className="font-mono text-xs text-slate-200 truncate">{h.sql_query}</div>
                    <div className="text-[10px] text-slate-400 mt-1 flex items-center gap-3">
                      <span>User: {h.username}</span>
                      <span>Time: {h.execution_time_ms} ms</span>
                      <span>{new Date(h.executed_at).toLocaleString()}</span>
                    </div>
                  </div>
                  <span className={`px-2 py-0.5 rounded text-[10px] font-bold ${
                    h.status === 'SUCCESS' ? 'bg-emerald-950 text-emerald-300' : 'bg-red-950 text-red-300'
                  }`}>
                    {h.status}
                  </span>
                </div>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  );
};
