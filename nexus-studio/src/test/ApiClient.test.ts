import { describe, it, expect, beforeEach } from 'vitest';
import { ApiClient } from '../api/client';

describe('ApiClient Session Management', () => {
  beforeEach(() => {
    localStorage.clear();
  });

  it('stores and retrieves session from localStorage', () => {
    expect(ApiClient.getSession()).toBeNull();

    ApiClient.setSession({
      token: 'mock-jwt-token',
      username: 'admin',
      role: 'ADMIN'
    });

    const session = ApiClient.getSession();
    expect(session).not.toBeNull();
    expect(session?.username).toBe('admin');
    expect(session?.role).toBe('ADMIN');
    expect(session?.token).toBe('mock-jwt-token');

    ApiClient.setSession(null);
    expect(ApiClient.getSession()).toBeNull();
  });
});
