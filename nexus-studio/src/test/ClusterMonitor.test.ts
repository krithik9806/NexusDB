import { describe, it, expect } from 'vitest';
import { NodeInfo } from '../api/client';

describe('Cluster Monitor Logic', () => {
  const sampleNodes: NodeInfo[] = [
    {
      nodeId: 'node-1',
      address: '127.0.0.1:9091',
      role: 'LEADER',
      status: 'ONLINE',
      uptimeSeconds: 3600,
      cacheHitRatio: 0.98,
      activeTransactions: 2,
      term: 3,
      commitIndex: 45,
      replicationLag: 0
    },
    {
      nodeId: 'node-2',
      address: '127.0.0.1:9092',
      role: 'FOLLOWER',
      status: 'ONLINE',
      uptimeSeconds: 3500,
      cacheHitRatio: 0.95,
      activeTransactions: 0,
      term: 3,
      commitIndex: 45,
      replicationLag: 4
    }
  ];

  it('identifies cluster leader and computes average cache hit ratio', () => {
    const leader = sampleNodes.find(n => n.role === 'LEADER');
    expect(leader).toBeDefined();
    expect(leader?.nodeId).toBe('node-1');
    expect(leader?.term).toBe(3);

    const followers = sampleNodes.filter(n => n.role !== 'LEADER');
    expect(followers).toHaveLength(1);
    expect(followers[0].replicationLag).toBe(4);

    const avgCacheHit = (sampleNodes.reduce((acc, n) => acc + n.cacheHitRatio, 0) / sampleNodes.length) * 100;
    expect(avgCacheHit).toBeCloseTo(96.5, 1);
  });
});
