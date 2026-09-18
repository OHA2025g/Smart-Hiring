import React, { useMemo, useState } from 'react';
import { Loader2 } from 'lucide-react';
import JobDetailMatchesEmpty from './JobDetailMatchesEmpty';
import JobDetailMatchCard from './JobDetailMatchCard';
import {
  computeMatchesKpis,
  matchesKpiStripItems,
  getMatchSourcesBanner,
  getSortLabel,
  filterUiMatchRows,
  isApifyPipelineActive,
} from '@/shared/lib/jobDetailMatchesUtils';

export default function JobDetailMatchesTab({
  job,
  matching,
  matchingCandidates,
  displayMatches,
  apifyPipeline,
  matchOrderMode,
  onFindMatches,
  onAddToPipeline,
  onGenerateDemo,
  demoGenerating,
}) {
  const [viewMode, setViewMode] = useState('grid');
  const safeMatches = useMemo(() => filterUiMatchRows(displayMatches), [displayMatches]);
  const safeAllMatches = useMemo(() => filterUiMatchRows(matchingCandidates), [matchingCandidates]);
  const hasMatches = safeMatches.length > 0;
  const kpis = useMemo(() => computeMatchesKpis(safeAllMatches), [safeAllMatches]);
  const kpiItems = useMemo(() => matchesKpiStripItems(kpis), [kpis]);
  const sourcesBanner = useMemo(() => getMatchSourcesBanner(safeAllMatches), [safeAllMatches]);

  const apifyRunning = isApifyPipelineActive(apifyPipeline);

  if (!hasMatches) {
    return (
      <div className="jd-match-tab jd-match-tab--empty" data-testid="job-matches-tab">
        <JobDetailMatchesEmpty
          job={job}
          matching={matching}
          apifyPipeline={apifyPipeline}
          onFindMatches={onFindMatches}
          onGenerateDemo={onGenerateDemo}
          demoGenerating={demoGenerating}
        />
      </div>
    );
  }

  return (
    <div className="jd-match-tab jd-match-tab--found" data-testid="job-matches-tab">
      {apifyRunning ? (
        <div className="jd-match-apify-banner" data-testid="apify-progress-banner">
          <Loader2 className="jd-match-apify-spinner" aria-hidden />
          LinkedIn search in progress via Apify ({apifyPipeline.status.replace('_', ' ')})…
        </div>
      ) : null}

      <div className="jd-match-banner">
        <div className="jd-match-banner-left">
          <span className="jd-match-af">AI</span>
          AI Matches generated from:{' '}
          <span className="jd-match-banner-muted">{sourcesBanner}</span>
        </div>
        <div className="jd-match-sort">
          <span className="jd-match-sort-label">
            Sorted by: <b>{getSortLabel(matchOrderMode)}</b>
            <span className="jd-match-sort-chevron" aria-hidden>
              ⌄
            </span>
          </span>
          <div className="jd-match-view-toggle">
            <button
              type="button"
              className={`jd-match-toggle ${viewMode === 'grid' ? 'active' : ''}`}
              aria-label="Grid view"
              onClick={() => setViewMode('grid')}
            >
              ▦
            </button>
            <button
              type="button"
              className={`jd-match-toggle ${viewMode === 'list' ? 'active' : ''}`}
              aria-label="List view"
              onClick={() => setViewMode('list')}
            >
              ☷
            </button>
          </div>
        </div>
      </div>

      <section className="jd-match-kpi-strip" data-testid="matches-found-kpis">
        {kpiItems.map((item) => (
          <div key={item.key} className="jd-match-kpi">
            <div className={`jd-match-icon ${item.circle}`}>{item.icon}</div>
            <div>
              <small>{item.label}</small>
              <b>{item.value}</b>
              {item.sub ? <span className={item.subClass}>{item.sub}</span> : null}
            </div>
          </div>
        ))}
      </section>

      <section
        className={`jd-match-grid ${viewMode === 'list' ? 'list' : ''}`}
        data-testid="job-matches-grid"
      >
        {safeMatches.map((match) => (
          <JobDetailMatchCard
            key={match.candidate.id}
            match={match}
            allMatches={safeAllMatches}
            onAddToPipeline={onAddToPipeline}
          />
        ))}
      </section>
    </div>
  );
}
