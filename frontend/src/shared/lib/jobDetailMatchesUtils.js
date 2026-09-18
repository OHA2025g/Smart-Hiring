import {
  isAiGeneratedCandidate,
  isExcelImportedCandidate,
  isRealLinkedInCandidate,
  isTalentPoolCandidate,
} from './candidateSource';
import { getMatchFinalScore } from './matchOrdering';

export const RECOMMENDED_POOL_SIZE = 50;
export const TARGET_FIT_LABEL = '80%+';
export const EXPECTED_SHORTLIST_LABEL = '12–18';
export const MIN_MATCH_SCORE = 70;

/** Apify pipeline statuses that mean LinkedIn ingest is still in flight. */
export const APIFY_PIPELINE_ACTIVE_STATUSES = [
  'search_running',
  'enrich_running',
  'email_running',
];

export function isApifyPipelineActive(pipeline) {
  return Boolean(pipeline && APIFY_PIPELINE_ACTIVE_STATUSES.includes(pipeline.status));
}

const FIT_TIERS = [
  { key: 'excellent', min: 80, label: 'Excellent Match', badge: 'Top Match', badgeClass: 'green' },
  { key: 'good', min: 60, label: 'Good Match', badge: 'Good Match', badgeClass: 'yellow' },
  { key: 'fair', min: 40, label: 'Fair Match', badge: 'Fair Match', badgeClass: 'blue' },
  { key: 'low', min: 0, label: 'Low Match', badge: 'Low Match', badgeClass: 'red' },
];

export function getMatchScore(match) {
  return getMatchFinalScore(match);
}

/** Keep only rows the job-detail UI can render (POST /match and enriched GET /matches). */
export function isUiMatchRow(match) {
  return Boolean(match?.candidate?.id && match?.fit_score);
}

export function filterUiMatchRows(matches = []) {
  if (!Array.isArray(matches)) return [];
  return matches.filter(isUiMatchRow);
}

export function getFitTier(score) {
  const n = Number(score) || 0;
  return FIT_TIERS.find((t) => n >= t.min) || FIT_TIERS[FIT_TIERS.length - 1];
}

export function getFitTierLabel(score) {
  return getFitTier(score).label;
}

export function getFitBadge(score) {
  const tier = getFitTier(score);
  return { label: tier.badge, className: tier.badgeClass };
}

export function getRingClass(score) {
  const n = Number(score) || 0;
  if (n >= 80) return '';
  if (n >= 60) return 'orange';
  if (n >= 40) return 'blue';
  return 'red';
}

export function getRingColor(score) {
  const n = Number(score) || 0;
  if (n >= 80) return '#16b981';
  if (n >= 60) return '#f59e0b';
  if (n >= 40) return '#2f80ed';
  return '#ef4444';
}

export function getBarFillClass(score) {
  const n = Number(score) || 0;
  if (n >= 60) return '';
  if (n >= 40) return 'orange';
  return 'red';
}

export function getSearchStatus({ matching, apifyPipeline, matchCount }) {
  if (matching) {
    return { label: 'Searching…', className: 'orange' };
  }
  if (isApifyPipelineActive(apifyPipeline)) {
    if (apifyPipeline.status === 'email_running') {
      return { label: 'Finding emails…', className: 'orange' };
    }
    return { label: 'LinkedIn in progress', className: 'orange' };
  }
  if (matchCount > 0) {
    return { label: 'Complete', className: 'green' };
  }
  if (
    apifyPipeline?.status === 'completed' &&
    (apifyPipeline.candidates_ingested || 0) > 0
  ) {
    return { label: 'Imported — scoring…', className: 'orange' };
  }
  return { label: 'Not started', className: 'orange' };
}

export function computeEmptyStateKpis({ matching, apifyPipeline, matchCount }) {
  const status = getSearchStatus({ matching, apifyPipeline, matchCount });
  return [
    { key: 'status', label: 'AI Search Status', value: status.label, className: status.className },
    { key: 'pool', label: 'Recommended Pool', value: String(RECOMMENDED_POOL_SIZE), className: 'purple' },
    { key: 'target', label: 'Target Fit Score', value: TARGET_FIT_LABEL, className: '' },
    { key: 'shortlist', label: 'Expected Shortlist', value: EXPECTED_SHORTLIST_LABEL, className: 'green' },
  ];
}

function pctOf(count, total) {
  if (!total) return 0;
  return Math.round((count / total) * 100);
}

export function computeMatchesKpis(matches = []) {
  const total = matches.length;
  let excellent = 0;
  let good = 0;
  let fair = 0;
  let low = 0;

  for (const m of matches) {
    const s = getMatchScore(m);
    if (s >= 80) excellent += 1;
    else if (s >= 60) good += 1;
    else if (s >= 40) fair += 1;
    else low += 1;
  }

  const sources = countDataSources(matches);

  return {
    total,
    excellent,
    good,
    fair,
    low,
    excellentPct: pctOf(excellent, total),
    goodPct: pctOf(good, total),
    fairPct: pctOf(fair, total),
    lowPct: pctOf(low, total),
    sourceCount: sources.count,
    sourceSummary: sources.summary,
  };
}

export function matchesKpiStripItems(kpis) {
  return [
    {
      key: 'total',
      icon: '♙',
      circle: 'purple',
      label: 'AI Matches Found',
      value: kpis.total,
      sub: 'High quality matches',
      subClass: 'green-t',
    },
    {
      key: 'excellent',
      icon: '◎',
      circle: 'green',
      label: 'Excellent Matches (80%+)',
      value: kpis.excellent,
      sub: `${kpis.excellentPct}% of total`,
      subClass: 'green-t',
    },
    {
      key: 'good',
      icon: '♙',
      circle: 'orange',
      label: 'Good Matches (60–79%)',
      value: kpis.good,
      sub: `${kpis.goodPct}% of total`,
      subClass: 'red-t',
    },
    {
      key: 'fair',
      icon: '♙',
      circle: 'blue',
      label: 'Fair Matches (40–59%)',
      value: kpis.fair,
      sub: `${kpis.fairPct}% of total`,
      subClass: 'blue-t',
    },
    {
      key: 'low',
      icon: '♙',
      circle: 'red',
      label: 'Low Matches (<40%)',
      value: kpis.low,
      sub: `${kpis.lowPct}% of total`,
      subClass: 'red-t',
    },
    {
      key: 'sources',
      icon: '☁',
      circle: 'purple',
      label: 'Data Sources',
      value: kpis.sourceCount,
      sub: kpis.sourceSummary,
      subClass: '',
    },
  ];
}

export function countDataSources(matches = []) {
  const flags = { excel: false, talent: false, ai: false, linkedin: false };
  for (const m of matches) {
    const c = m?.candidate;
    if (isExcelImportedCandidate(c)) flags.excel = true;
    else if (isTalentPoolCandidate(c)) flags.talent = true;
    else if (isRealLinkedInCandidate(c)) flags.linkedin = true;
    else if (isAiGeneratedCandidate(c)) flags.ai = true;
  }
  const labels = [];
  if (flags.excel) labels.push('Excel');
  if (flags.talent) labels.push('Talent Pool');
  if (flags.ai || flags.linkedin) labels.push('AI');
  return {
    count: labels.length || 1,
    summary: labels.length ? labels.join(', ') : 'Talent Pool',
  };
}

export function getMatchSourcesBanner(matches = []) {
  const parts = [];
  if (matches.some((m) => isExcelImportedCandidate(m?.candidate))) parts.push('Excel import');
  if (matches.some((m) => isTalentPoolCandidate(m?.candidate))) parts.push('Talent pool');
  if (matches.some((m) => isAiGeneratedCandidate(m?.candidate))) parts.push('AI generated');
  if (matches.some((m) => isRealLinkedInCandidate(m?.candidate))) parts.push('LinkedIn');
  return parts.length ? parts.join(' · ') : 'Talent database';
}

export function getRoleAverageScore(matches = []) {
  if (!matches.length) return 0;
  const sum = matches.reduce((acc, m) => acc + getMatchScore(m), 0);
  return Math.round(sum / matches.length);
}

export function getVsRoleAverageDelta(score, matches = []) {
  const avg = getRoleAverageScore(matches);
  const delta = Math.round(score - avg);
  if (delta > 0) return { text: `↑ ${delta}% vs role average`, className: 'green-t' };
  if (delta < 0) return { text: `↓ ${Math.abs(delta)}% vs role average`, className: 'red-t' };
  return { text: 'At role average', className: 'orange-t' };
}

export function getFitSubtitle(score, fitScore, matches = []) {
  if (!fitScore?.must_have_ok) {
    return { text: 'Missing required skills', className: 'red-t' };
  }
  if (score >= 40 && score < 60) {
    return { text: 'Review before adding', className: 'orange-t' };
  }
  return getVsRoleAverageDelta(score, matches);
}

export function formatCandidateLocation(candidate) {
  const exp = candidate?.total_experience_years;
  const expLabel =
    exp != null && !Number.isNaN(Number(exp))
      ? `${Math.floor(Number(exp))}+ yrs exp.`
      : null;
  const loc = candidate?.location?.trim();
  if (expLabel && loc) return `${expLabel} · ${loc}`;
  if (expLabel) return expLabel;
  if (loc) return loc;
  return candidate?.headline || candidate?.email || '—';
}

export function getSourceTagClass(candidate) {
  if (isRealLinkedInCandidate(candidate) || isAiGeneratedCandidate(candidate)) return 'blue';
  if (isTalentPoolCandidate(candidate)) return 'green';
  return 'blue';
}

export function getSourceTagLabel(candidate) {
  if (isRealLinkedInCandidate(candidate)) return 'LinkedIn';
  if (isTalentPoolCandidate(candidate)) return 'In DB';
  if (isAiGeneratedCandidate(candidate)) return 'LinkedIn';
  const source = String(candidate?.source || '').trim();
  if (source) return source.replace(/_/g, ' ');
  return 'Talent Pool';
}

function skillLabel(item) {
  if (typeof item === 'string') return item;
  if (item?.skill_name) return String(item.skill_name);
  return String(item ?? '');
}

export function buildInsightSection(fitScore) {
  const matched = (fitScore?.explanation?.matched_skills || []).map(skillLabel);
  const missing = (fitScore?.explanation?.missing_must_have || []).map(skillLabel);
  const strengths =
    matched.length > 0
      ? matched.slice(0, 3)
      : (fitScore?.explanation?.strengths || []).slice(0, 3).map(skillLabel);

  if (missing.length > 0) {
    return {
      title: 'Gaps Identified',
      rows: missing.slice(0, 3).map((label) => ({ label, tone: 'red', icon: '!' })),
    };
  }
  if (strengths.length > 0) {
    const tone = getMatchScore({ fit_score: fitScore }) >= 80 ? 'green' : 'orange';
    return {
      title: 'Key Strengths',
      rows: strengths.map((label) => ({ label, tone, icon: '✓' })),
    };
  }
  return null;
}

export function getMatchedSkillsDisplay(fitScore, limit = 3) {
  const matched = (fitScore?.explanation?.matched_skills || []).map(skillLabel);
  const missing = (fitScore?.explanation?.missing_must_have || []).map(skillLabel);
  if (missing.length > 0) {
    return { label: 'Missing Skills', skills: missing, variant: 'red' };
  }
  if (matched.length === 0) return null;
  const visible = matched.slice(0, limit);
  const extra = matched.length - visible.length;
  return { label: 'Matched Skills', skills: visible, extra, variant: 'green' };
}

export function sortMatchesByScore(matches = []) {
  return [...matches].sort((a, b) => getMatchScore(b) - getMatchScore(a));
}

export function getSortLabel(matchOrderMode) {
  if (matchOrderMode === 'linkedin_first') return 'LinkedIn first';
  return 'Overall Fit';
}

export function canGenerateDemo(job) {
  return Boolean(job?.title && job?.description && job?.skills?.length > 0);
}

export function emptyRecommendationCopy(job) {
  const signal =
    job?.business_department ||
    job?.domain ||
    job?.seniority ||
    'role';
  const slug = String(signal).toLowerCase().replace(/\s+/g, '-');
  return `Run matching now. This role has a strong ${slug} signal but no AI-ranked candidate pool yet.`;
}
