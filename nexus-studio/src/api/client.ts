export interface UserSession {
  token: string;
  username: string;
  role: 'ADMIN' | 'DEVELOPER' | 'VIEWER';
}

export interface NodeInfo {
  nodeId: string;
  address: string;
  role: 'LEADER' | 'FOLLOWER';
  status: string;
  uptimeSeconds: number;
  cacheHitRatio: number;
  activeTransactions: number;
  term: number;
  commitIndex: number;
  replicationLag: number;
}

export interface SqlResult {
  columns: string[];
  rows: string[][];
  rowsAffected: number;
  executionTimeMs: number;
  error?: string;
}

export interface TableColumn {
  name: string;
  type: string;
  isNullable: string;
}

export interface TableSchema {
  tableName: string;
  columns: TableColumn[];
  primaryKey: string;
  indexes: string[];
}

export interface MetricPoint {
  timestamp: number;
  throughputOpsPerSec: number;
  avgLatencyMs: number;
  p95LatencyMs: number;
  p99LatencyMs: number;
  cacheHitRatio: number;
  memoryUsedBytes: number;
  activeTransactions: number;
  replicationLag: number;
}

const API_BASE = '/api';

export class ApiClient {
  private static tokenKey = 'nexus_token';
  private static userKey = 'nexus_user';

  static getSession(): UserSession | null {
    const token = localStorage.getItem(this.tokenKey);
    const user = localStorage.getItem(this.userKey);
    if (!token || !user) return null;
    try {
      const parsed = JSON.parse(user);
      return { token, username: parsed.username, role: parsed.role };
    } catch {
      return null;
    }
  }

  static setSession(session: UserSession | null) {
    if (!session) {
      localStorage.removeItem(this.tokenKey);
      localStorage.removeItem(this.userKey);
    } else {
      localStorage.setItem(this.tokenKey, session.token);
      localStorage.setItem(this.userKey, JSON.stringify({ username: session.username, role: session.role }));
    }
  }

  private static async request<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
    const session = this.getSession();
    const headers = new Headers(options.headers || {});
    headers.set('Content-Type', 'application/json');
    if (session?.token) {
      headers.set('Authorization', `Bearer ${session.token}`);
    }

    const res = await fetch(`${API_BASE}${endpoint}`, {
      ...options,
      headers,
    });

    if (res.status === 401) {
      this.setSession(null);
      window.dispatchEvent(new Event('nexus:unauthorized'));
      throw new Error('Unauthorized');
    }

    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: res.statusText }));
      throw new Error(err.error || `HTTP ${res.status}`);
    }

    return res.json();
  }

  static async login(username: string, password: string): Promise<UserSession> {
    const res = await fetch(`${API_BASE}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({ error: 'Login failed' }));
      throw new Error(err.error || 'Invalid credentials');
    }
    const data = await res.json();
    const session: UserSession = {
      token: data.token,
      username: data.username,
      role: data.role
    };
    this.setSession(session);
    return session;
  }

  static async getTopology(): Promise<NodeInfo[]> {
    return this.request<NodeInfo[]>('/cluster/topology');
  }

  static async getNode(id: string): Promise<NodeInfo> {
    return this.request<NodeInfo>(`/nodes/${id}`);
  }

  static async executeSql(sql: string): Promise<SqlResult> {
    return this.request<SqlResult>('/sql/execute', {
      method: 'POST',
      body: JSON.stringify({ sql })
    });
  }

  static async explainSql(sql: string): Promise<{ explanation: string }> {
    return this.request<{ explanation: string }>('/sql/explain', {
      method: 'POST',
      body: JSON.stringify({ sql })
    });
  }

  static async getQueryHistory(limit = 20): Promise<any[]> {
    return this.request<any[]>(`/sql/history?limit=${limit}`);
  }

  static async getSchema(): Promise<TableSchema[]> {
    return this.request<TableSchema[]>('/schema');
  }

  static async getTimeSeries(): Promise<MetricPoint[]> {
    return this.request<MetricPoint[]>('/metrics/timeseries');
  }

  static async injectFault(targetNode: string, action: string, durationMs: number = 2000): Promise<any> {
    return this.request<any>('/faults/action', {
      method: 'POST',
      body: JSON.stringify({ targetNode, action, durationMs })
    });
  }

  static async getAuditLog(limit = 20): Promise<any[]> {
    return this.request<any[]>(`/faults/audit-log?limit=${limit}`);
  }
}
