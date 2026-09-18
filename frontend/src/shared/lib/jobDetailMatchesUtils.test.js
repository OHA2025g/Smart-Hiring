import { describe, expect, it } from 'vitest';
import {
  getSearchStatus,
  isApifyPipelineActive,
} from './jobDetailMatchesUtils';

describe('getSearchStatus', () => {
  it('treats email_running as active LinkedIn work', () => {
    expect(isApifyPipelineActive({ status: 'email_running' })).toBe(true);
    expect(getSearchStatus({ matching: false, apifyPipeline: { status: 'email_running' }, matchCount: 0 })).toEqual({
      label: 'Finding emails…',
      className: 'orange',
    });
  });

  it('does not show Not started when Apify imported profiles but matches are empty', () => {
    const status = getSearchStatus({
      matching: false,
      apifyPipeline: { status: 'completed', candidates_ingested: 25 },
      matchCount: 0,
    });
    expect(status.label).toBe('Imported — scoring…');
  });

  it('shows Complete when matches exist', () => {
    expect(
      getSearchStatus({
        matching: false,
        apifyPipeline: { status: 'completed', candidates_ingested: 25 },
        matchCount: 12,
      }).label
    ).toBe('Complete');
  });
});
