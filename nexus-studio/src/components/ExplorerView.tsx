import React, { useEffect, useState } from 'react';
import { ApiClient, TableSchema, SqlResult } from '../api/client';
import { Database, Table, Key, Hash, Eye, RefreshCw } from 'lucide-react';

export const ExplorerView: React.FC = () => {
  const [tables, setTables] = useState<TableSchema[]>([]);
  const [selectedTable, setSelectedTable] = useState<TableSchema | null>(null);
  const [previewResult, setPreviewResult] = useState<SqlResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [previewLoading, setPreviewLoading] = useState(false);

  const fetchSchema = async () => {
    setLoading(true);
    try {
      const data = await ApiClient.getSchema();
      setTables(data);
      if (data.length > 0 && !selectedTable) {
        setSelectedTable(data[0]);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSchema();
  }, []);

  const handlePreview = async (tableName: string) => {
    setPreviewLoading(true);
    try {
      const res = await ApiClient.executeSql(`SELECT * FROM ${tableName} LIMIT 10;`);
      setPreviewResult(res);
    } catch (ignored) {
      setPreviewResult(null);
    } finally {
      setPreviewLoading(false);
    }
  };

  useEffect(() => {
    if (selectedTable) {
      handlePreview(selectedTable.tableName);
    }
  }, [selectedTable]);

  return (
    <div className="flex-1 flex overflow-hidden bg-nexus-950">
      {/* Tables List Panel */}
      <div className="w-64 border-r border-nexus-800 bg-nexus-900 flex flex-col">
        <div className="h-12 border-b border-nexus-800 px-4 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Database className="w-4 h-4 text-nexus-accent" />
            <span className="text-xs font-bold text-white tracking-wide">Catalog Tables</span>
          </div>
          <button
            onClick={fetchSchema}
            disabled={loading}
            className="p-1 text-slate-400 hover:text-slate-200 rounded"
            title="Refresh schema"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>

        <div className="flex-1 overflow-y-auto p-2 space-y-1">
          {tables.length === 0 ? (
            <div className="text-center py-6 text-slate-500 text-xs">No tables in catalog.</div>
          ) : (
            tables.map((t) => (
              <button
                key={t.tableName}
                onClick={() => setSelectedTable(t)}
                className={`w-full flex items-center justify-between px-3 py-2 rounded-lg text-xs transition ${
                  selectedTable?.tableName === t.tableName
                    ? 'bg-nexus-800 text-nexus-accent border border-nexus-700 font-semibold shadow-sm'
                    : 'text-slate-300 hover:bg-nexus-850'
                }`}
              >
                <div className="flex items-center gap-2 truncate">
                  <Table className="w-3.5 h-3.5 shrink-0 text-slate-400" />
                  <span className="font-mono truncate">{t.tableName}</span>
                </div>
                <span className="text-[10px] font-mono text-slate-500">
                  {t.columns.length} cols
                </span>
              </button>
            ))
          )}
        </div>
      </div>

      {/* Selected Table Details Panel */}
      <div className="flex-1 flex flex-col overflow-y-auto p-6 space-y-6">
        {selectedTable ? (
          <>
            {/* Header */}
            <div className="flex items-center justify-between">
              <div>
                <div className="flex items-center gap-2">
                  <h1 className="text-xl font-bold font-mono text-white">{selectedTable.tableName}</h1>
                  <span className="text-[10px] font-mono uppercase px-2 py-0.5 bg-nexus-800 text-slate-300 rounded border border-nexus-700">
                    Heap Stored
                  </span>
                </div>
                <p className="text-xs text-slate-400 mt-1">
                  Primary Key: <span className="font-mono text-amber-300 font-semibold">{selectedTable.primaryKey || 'None'}</span>
                </p>
              </div>
              <button
                onClick={() => handlePreview(selectedTable.tableName)}
                disabled={previewLoading}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-nexus-800 hover:bg-nexus-700 border border-nexus-700 rounded-lg text-xs font-semibold text-slate-200 transition"
              >
                <Eye className="w-3.5 h-3.5 text-nexus-accent" />
                <span>{previewLoading ? 'Loading...' : 'Refresh Preview'}</span>
              </button>
            </div>

            {/* Schema Columns Table */}
            <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-5">
              <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3">Columns Definition</h2>
              <table className="w-full text-left text-xs">
                <thead>
                  <tr className="border-b border-nexus-800 text-slate-400">
                    <th className="pb-2 font-medium">Column Name</th>
                    <th className="pb-2 font-medium">Data Type</th>
                    <th className="pb-2 font-medium">Attributes</th>
                    <th className="pb-2 font-medium">Nullable</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-nexus-850 font-mono">
                  {selectedTable.columns.map((col, idx) => (
                    <tr key={idx} className="hover:bg-nexus-850/40">
                      <td className="py-2.5 font-bold text-slate-200 flex items-center gap-1.5">
                        {col.name === selectedTable.primaryKey ? (
                          <Key className="w-3.5 h-3.5 text-amber-400 shrink-0" />
                        ) : (
                          <span className="w-3.5" />
                        )}
                        <span>{col.name}</span>
                      </td>
                      <td className="py-2.5 text-nexus-accent">{col.type}</td>
                      <td className="py-2.5">
                        {col.name === selectedTable.primaryKey && (
                          <span className="px-1.5 py-0.5 bg-amber-400/10 text-amber-300 border border-amber-400/30 rounded text-[10px] font-bold">
                            PRIMARY KEY
                          </span>
                        )}
                      </td>
                      <td className="py-2.5 text-slate-400">
                        {col.isNullable === 'true' ? 'YES' : 'NO'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Indexes Section */}
            {selectedTable.indexes.length > 0 && (
              <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-5">
                <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400 mb-3">Indexes (B+ Tree)</h2>
                <div className="flex flex-wrap gap-2">
                  {selectedTable.indexes.map((idx, i) => (
                    <div
                      key={i}
                      className="flex items-center gap-1.5 px-3 py-1.5 bg-nexus-850 border border-nexus-700 rounded-lg text-xs font-mono text-purple-300"
                    >
                      <Hash className="w-3.5 h-3.5 text-purple-400" />
                      <span>{idx}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Row Preview Table */}
            <div className="bg-nexus-900 border border-nexus-800 rounded-xl p-5">
              <div className="flex items-center justify-between mb-3">
                <h2 className="text-xs font-bold uppercase tracking-wider text-slate-400">
                  Data Preview (First 10 Rows)
                </h2>
                {previewResult && (
                  <span className="text-[11px] font-mono text-slate-400">
                    Fetched in {previewResult.executionTimeMs} ms
                  </span>
                )}
              </div>

              {previewResult && previewResult.columns.length > 0 ? (
                <div className="overflow-x-auto border border-nexus-800 rounded-lg">
                  <table className="w-full text-left text-xs font-mono">
                    <thead className="bg-nexus-850 border-b border-nexus-800">
                      <tr>
                        {previewResult.columns.map((c, idx) => (
                          <th key={idx} className="py-2 px-3 text-slate-300 font-semibold">
                            {c}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-nexus-850">
                      {previewResult.rows.length === 0 ? (
                        <tr>
                          <td colSpan={previewResult.columns.length} className="py-4 text-center text-slate-500">
                            Table is currently empty.
                          </td>
                        </tr>
                      ) : (
                        previewResult.rows.map((row, rIdx) => (
                          <tr key={rIdx} className="hover:bg-nexus-850/50">
                            {row.map((val, cIdx) => (
                              <td key={cIdx} className="py-2 px-3 text-slate-200 whitespace-nowrap">
                                {val}
                              </td>
                            ))}
                          </tr>
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              ) : (
                <div className="text-center py-6 text-slate-500 text-xs">
                  {previewLoading ? 'Loading preview...' : 'No data preview available.'}
                </div>
              )}
            </div>
          </>
        ) : (
          <div className="h-full flex flex-col items-center justify-center text-slate-500 text-xs">
            <Database className="w-8 h-8 text-slate-600 mb-2" />
            <span>Select a table from the catalog list to inspect its schema and data.</span>
          </div>
        )}
      </div>
    </div>
  );
};
