import { describe, it, expect } from 'vitest';
import { parseExplainOutput } from '../components/QueryAnalyzerView';

describe('Query Plan Parser', () => {
  it('parses single operator with cost and rows', () => {
    const text = 'SeqScan on users (cost=12.50 rows=100)';
    const nodes = parseExplainOutput(text);
    expect(nodes).toHaveLength(1);
    expect(nodes[0].operator).toBe('SeqScan');
    expect(nodes[0].cost).toBe('12.50');
    expect(nodes[0].rows).toBe('100');
    expect(nodes[0].depth).toBe(0);
  });

  it('parses nested tree structure with indentation', () => {
    const text = `
HashJoin on (users.id = orders.user_id) (cost=45.20 rows=50)
  Filter on (users.active = true) (cost=15.00 rows=80)
    IndexScan on users (idx_users_id) (cost=5.00 rows=80)
  SeqScan on orders (cost=20.00 rows=1000)
    `.trim();

    const nodes = parseExplainOutput(text);
    expect(nodes).toHaveLength(4);
    expect(nodes[0].operator).toBe('HashJoin');
    expect(nodes[0].depth).toBe(0);

    expect(nodes[1].operator).toBe('Filter');
    expect(nodes[1].depth).toBe(1);

    expect(nodes[2].operator).toBe('IndexScan');
    expect(nodes[2].depth).toBe(2);

    expect(nodes[3].operator).toBe('SeqScan');
    expect(nodes[3].depth).toBe(1);
  });
});
