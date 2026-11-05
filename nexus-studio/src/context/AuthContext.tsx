import React, { createContext, useContext, useEffect, useState } from 'react';
import { ApiClient, UserSession } from '../api/client';

interface AuthContextType {
  session: UserSession | null;
  login: (u: string, p: string) => Promise<void>;
  logout: () => void;
  isAuthenticated: boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [session, setSession] = useState<UserSession | null>(() => ApiClient.getSession());

  useEffect(() => {
    const handleUnauthorized = () => setSession(null);
    window.addEventListener('nexus:unauthorized', handleUnauthorized);
    return () => window.removeEventListener('nexus:unauthorized', handleUnauthorized);
  }, []);

  const login = async (u: string, p: string) => {
    const userSession = await ApiClient.login(u, p);
    setSession(userSession);
  };

  const logout = () => {
    ApiClient.setSession(null);
    setSession(null);
  };

  return (
    <AuthContext.Provider value={{ session, login, logout, isAuthenticated: !!session }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
