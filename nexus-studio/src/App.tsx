import React, { useState } from 'react';
import { AuthProvider, useAuth } from './context/AuthContext';
import { Sidebar, ActiveTab } from './components/Sidebar';
import { LoginModal } from './components/LoginModal';
import { DashboardView } from './components/DashboardView';
import { SqlEditorView } from './components/SqlEditorView';
import { ExplorerView } from './components/ExplorerView';
import { QueryAnalyzerView } from './components/QueryAnalyzerView';

const StudioMain: React.FC = () => {
  const { isAuthenticated } = useAuth();
  const [activeTab, setActiveTab] = useState<ActiveTab>('dashboard');

  if (!isAuthenticated) {
    return <LoginModal />;
  }

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-nexus-950 text-slate-100 font-sans">
      <Sidebar activeTab={activeTab} setActiveTab={setActiveTab} />
      <main className="flex-1 flex flex-col overflow-hidden">
        {activeTab === 'dashboard' && <DashboardView />}
        {activeTab === 'sql' && <SqlEditorView />}
        {activeTab === 'explorer' && <ExplorerView />}
        {activeTab === 'analyzer' && <QueryAnalyzerView />}
        {(activeTab === 'cluster' || activeTab === 'performance' || activeTab === 'faults') && (
          <DashboardView />
        )}
      </main>
    </div>
  );
};

export const App: React.FC = () => {
  return (
    <AuthProvider>
      <StudioMain />
    </AuthProvider>
  );
};
