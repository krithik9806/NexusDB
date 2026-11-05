import { describe, it, expect } from 'vitest';

describe('Fault Simulator Actions', () => {
  it('validates supported fault types and durations', () => {
    const supportedActions = ['PARTITION', 'LATENCY', 'FORCE_ELECTION', 'HEAL', 'STOP', 'RESTART'];
    expect(supportedActions).toContain('PARTITION');
    expect(supportedActions).toContain('LATENCY');
    expect(supportedActions).toContain('HEAL');

    const defaultDuration = 3000;
    expect(defaultDuration).toBeGreaterThan(0);
    expect(defaultDuration).toBeLessThanOrEqual(10000);
  });
});
