import React from 'react';
import { useAuth } from '../context/AuthContext';
import {
  LayoutDashboard,
  Terminal,
  Database,
  GitGraph,
  Network,
  Activity,
  AlertTriangle,
  LogOut,
  Layers
} from 'lucide-react';

export type ActiveTab =
  | 'dashboard'
  | 'sql'
  | 'explorer'
  | 'analyzer'
  | 'cluster'
  | 'performance'
  | 'faults';

interface SidebarProps {
  activeTab: ActiveTab;
  setActiveTab: (tab: ActiveTab) => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ activeTab, setActiveTab }) => {
  const { session, logout } = useAuth();

  const navItems = [
    { id: 'dashboard' as ActiveTab, label: 'Overview', icon: LayoutDashboard },
    { id: 'sql' as ActiveTab, label: 'SQL Editor', icon: Terminal },
    { id: 'explorer' as ActiveTab, label: 'Explorer', icon: Database },
    { id: 'analyzer' as ActiveTab, label: 'Query Analyzer', icon: GitGraph },
    { id: 'cluster' as ActiveTab, label: 'Cluster Monitor', icon: Network },
    { id: 'performance' as ActiveTab, label: 'Performance', icon: Activity },
    { id: 'faults' as ActiveTab, label: 'Fault Simulator', icon: AlertTriangle, adminOnly: true },
  ];

  return (
    <aside className="w-64 bg-nexus-900 border-r border-nexus-800 flex flex-col justify-between select-none">
      <div>
        {/* Brand Header */}
        <div className="h-14 border-b border-nexus-800 flex items-center px-5 gap-3">
          <div className="w-8 h-8 rounded-lg bg-gradient-to-tr from-nexus-accentDark to-nexus-accent flex items-center justify-center text-nexus-950 font-black shadow-md shadow-nexus-accent/20">
            <Layers className="w-5 h-5 text-nexus-950" />
          </div>
          <div>
            <div className="text-sm font-bold text-white tracking-wide">NEXUS<span className="text-nexus-accent">DB</span></div>
            <div className="text-[10px] text-slate-400 font-mono tracking-wider">STUDIO v0.8</div>
          </div>
        </div>

        {/* Navigation */}
        <nav className="p-3 space-y-1">
          <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider font-semibold text-slate-500">
            Core Tools
          </div>
          {navItems.map((item) => {
            if (item.adminOnly && session?.role !== 'ADMIN') return null;
            const Icon = item.icon;
            const isActive = activeTab === item.id;
            return (
              <button
                key={item.id}
                onClick={() => setActiveTab(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg text-xs font-medium transition duration-150 ${
                  isActive
                    ? 'bg-nexus-800 text-nexus-accent border border-nexus-700 shadow-sm'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-nexus-850'
                }`}
              >
                <Icon className={`w-4 h-4 ${isActive ? 'text-nexus-accent' : 'text-slate-400'}`} />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
      </div>

      {/* User / Session Footer */}
      <div className="p-3 border-t border-nexus-800">
        <div className="bg-nexus-850 border border-nexus-800 rounded-lg p-2.5 flex items-center justify-between mb-2">
          <div className="overflow-hidden">
            <div className="text-xs font-semibold text-white truncate">{session?.username}</div>
            <div className="flex items-center gap-1.5 mt-0.5">
              <span className={`inline-block w-1.5 h-1.5 rounded-full ${
                session?.role === 'ADMIN' ? 'bg-purple-400' : session?.role === 'DEVELOPER' ? 'bg-blue-400' : 'bg-emerald-400'
              }`} />
              <span className="text-[10px] font-mono text-slate-400 uppercase tracking-wider">{session?.role}</span>
            </div>
          </div>
          <button
            onClick={logout}
            title="Sign out"
            className="p-1.5 text-slate-400 hover:text-red-400 hover:bg-nexus-800 rounded transition"
          >
            <LogOut className="w-4 h-4" />
          </button>
        </div>
      </div>
    </aside>
  );
};
