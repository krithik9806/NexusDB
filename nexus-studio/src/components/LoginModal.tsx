import React, { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { Shield, KeyRound, User, AlertCircle } from 'lucide-react';

export const LoginModal: React.FC = () => {
  const { login } = useAuth();
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('admin123');
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      await login(username, password);
    } catch (err: any) {
      setError(err.message || 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  const handleQuickLogin = (u: string, p: string) => {
    setUsername(u);
    setPassword(p);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm p-4">
      <div className="bg-nexus-900 border border-nexus-700 rounded-xl p-8 max-w-md w-full shadow-2xl">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-nexus-accent/10 border border-nexus-accent/30 rounded-lg text-nexus-accent">
            <Shield className="w-6 h-6" />
          </div>
          <div>
            <h2 className="text-xl font-bold tracking-tight text-white">NexusDB Studio</h2>
            <p className="text-xs text-slate-400">Authentication & Control Plane Access</p>
          </div>
        </div>

        {error && (
          <div className="mb-4 p-3 bg-red-950/50 border border-red-800 rounded-lg text-red-200 text-xs flex items-center gap-2">
            <AlertCircle className="w-4 h-4 shrink-0" />
            <span>{error}</span>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-300 mb-1.5">
              Username
            </label>
            <div className="relative">
              <User className="w-4 h-4 text-slate-400 absolute left-3 top-3" />
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
                className="w-full bg-nexus-850 border border-nexus-700 rounded-lg pl-9 pr-3 py-2 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-nexus-accent focus:ring-1 focus:ring-nexus-accent"
                placeholder="Enter username"
              />
            </div>
          </div>

          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-300 mb-1.5">
              Password
            </label>
            <div className="relative">
              <KeyRound className="w-4 h-4 text-slate-400 absolute left-3 top-3" />
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                className="w-full bg-nexus-850 border border-nexus-700 rounded-lg pl-9 pr-3 py-2 text-sm text-slate-100 placeholder-slate-500 focus:outline-none focus:border-nexus-accent focus:ring-1 focus:ring-nexus-accent"
                placeholder="••••••••"
              />
            </div>
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full py-2.5 px-4 bg-nexus-accent hover:bg-nexus-accentDark text-nexus-950 font-bold rounded-lg text-sm transition duration-150 disabled:opacity-50 mt-2 shadow-lg shadow-nexus-accent/10"
          >
            {loading ? 'Authenticating...' : 'Sign In'}
          </button>
        </form>

        <div className="mt-6 pt-5 border-t border-nexus-800">
          <p className="text-[11px] text-slate-400 mb-2 font-medium">Quick switch accounts:</p>
          <div className="grid grid-cols-3 gap-2">
            <button
              onClick={() => handleQuickLogin('admin', 'admin123')}
              className="py-1.5 px-2 bg-nexus-800 hover:bg-nexus-700 rounded border border-nexus-700 text-[11px] text-slate-300 transition"
            >
              ADMIN
            </button>
            <button
              onClick={() => handleQuickLogin('dev', 'dev123')}
              className="py-1.5 px-2 bg-nexus-800 hover:bg-nexus-700 rounded border border-nexus-700 text-[11px] text-slate-300 transition"
            >
              DEVELOPER
            </button>
            <button
              onClick={() => handleQuickLogin('viewer', 'viewer123')}
              className="py-1.5 px-2 bg-nexus-800 hover:bg-nexus-700 rounded border border-nexus-700 text-[11px] text-slate-300 transition"
            >
              VIEWER
            </button>
          </div>
        </div>
      </div>
    </div>
  );
};
