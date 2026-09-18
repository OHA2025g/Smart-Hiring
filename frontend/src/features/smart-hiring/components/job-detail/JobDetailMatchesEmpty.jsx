import React from 'react';
import { Loader2 } from 'lucide-react';
import {
  MIN_MATCH_SCORE,
  RECOMMENDED_POOL_SIZE,
  computeEmptyStateKpis,
  canGenerateDemo,
  emptyRecommendationCopy,
  isApifyPipelineActive,
} from '@/shared/lib/jobDetailMatchesUtils';

export default function JobDetailMatchesEmpty({
  job,
  matching,
  apifyPipeline,
  onFindMatches,
  onGenerateDemo,
  demoGenerating,
}) {
  const kpis = computeEmptyStateKpis({ matching, apifyPipeline, matchCount: 0 });
  const apifyRunning = isApifyPipelineActive(apifyPipeline);

  return (
    <>
      {apifyRunning ? (
        <div className="jd-match-apify-banner" data-testid="apify-progress-banner">
          <Loader2 className="jd-match-apify-spinner" aria-hidden />
          LinkedIn search in progress via Apify ({apifyPipeline.status.replace('_', ' ')})…
        </div>
      ) : null}

      <section className="jd-match-empty-kpis" data-testid="matches-empty-kpis">
        {kpis.map((kpi) => (
          <div key={kpi.key} className="jd-match-empty-kpi">
            <small>{kpi.label}</small>
            <b className={kpi.className || undefined}>{kpi.value}</b>
          </div>
        ))}
      </section>

      <section className="jd-match-ai-shell">
        <div className="jd-match-empty-card">
          <div className="jd-match-empty-inner">
            <div className="jd-match-target" aria-hidden>
              ◎
            </div>
            <h2>No AI matches yet</h2>
            <p>
              Start AI matching to discover high-fit candidates for this role using skills, title
              similarity, seniority, work mode, and career trajectory signals.
            </p>
            <button
              type="button"
              className="jd-match-cta"
              onClick={onFindMatches}
              disabled={matching}
              data-testid="find-ai-matches-btn"
            >
              {matching ? (
                <>
                  <Loader2 className="jd-match-btn-spinner" aria-hidden />
                  Finding matches…
                </>
              ) : (
                '✣ Find AI Matches'
              )}
            </button>
          </div>
        </div>

        <aside className="jd-match-side-panel">
          <div className="jd-match-panel">
            <h3>How matching will work</h3>
            <p>
              The AI engine ranks candidates against this requisition using multiple evidence signals
              instead of keyword match only.
            </p>
            <div className="jd-match-steps">
              <div className="jd-match-step">
                <div className="jd-match-num">1</div>
                <div>
                  <b>Parse role requirements</b>
                  <br />
                  <span>Leadership, communication, decision-making, and stakeholder signals.</span>
                </div>
              </div>
              <div className="jd-match-step">
                <div className="jd-match-num">2</div>
                <div>
                  <b>Score candidate fit</b>
                  <br />
                  <span>Skills, title, activity, experience, and role proximity.</span>
                </div>
              </div>
              <div className="jd-match-step">
                <div className="jd-match-num">3</div>
                <div>
                  <b>Recommend next action</b>
                  <br />
                  <span>Shortlist, review, assess, or keep in talent pool.</span>
                </div>
              </div>
            </div>
          </div>
          <div className="jd-match-panel jd-match-panel-config">
            <h3>Match configuration</h3>
            <p>
              <b>Minimum score:</b> {MIN_MATCH_SCORE}%
            </p>
            <p>
              <b>Source:</b> LinkedIn + Talent Pool
            </p>
            <p>
              <b>Output:</b> {RECOMMENDED_POOL_SIZE} ranked profiles
            </p>
          </div>
        </aside>
      </section>

      <div className="jd-match-recommend">
        <div>
          <h3>AI recommendation</h3>
          <p>{emptyRecommendationCopy(job)}</p>
        </div>
        <button
          type="button"
          className="jd-match-cta"
          onClick={onGenerateDemo}
          disabled={demoGenerating || matching || !canGenerateDemo(job)}
          data-testid="generate-shortlist-btn"
        >
          {demoGenerating ? 'Generating…' : 'Generate ranked shortlist →'}
        </button>
      </div>
    </>
  );
}
