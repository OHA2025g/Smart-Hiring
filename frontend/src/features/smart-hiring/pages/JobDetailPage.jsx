import React, { useState, useEffect, useMemo, useRef } from 'react';
import { useParams, Link, useNavigate, useSearchParams } from 'react-router-dom';
import { jobsApi, applicationsApi } from '@/shared/lib/api';
import { Card, CardContent } from '@/shared/ui/card';
import { Button } from '@/shared/ui/button';
import { Badge } from '@/shared/ui/badge';
import { useCareerTrajectorySummaries } from '@/shared/hooks/useCareerTrajectorySummaries';
import { useAssessmentClearance } from '@/shared/hooks/useAssessmentClearance';
import {
  Users,
  Target,
  Plus,
  Loader2,
} from 'lucide-react';
import { toast } from 'sonner';
import { getCandidateCardBadge } from '@/shared/lib/candidateSource';
import { orderJobMatchesForGrid, orderJobMatchesLinkedInFirst } from '@/shared/lib/matchOrdering';
import { useAuth } from '@/shared/context/AuthContext';
import { useHiringPermissions } from '@/shared/hooks/useHiringPermissions';
import JobDetailOverviewTab from '@/features/smart-hiring/components/job-detail/JobDetailOverviewTab';
import JobDetailCandidatesTab from '@/features/smart-hiring/components/job-detail/JobDetailCandidatesTab';
import JobDetailMatchesTab from '@/features/smart-hiring/components/job-detail/JobDetailMatchesTab';
import { statusBadgeClass } from '@/shared/lib/jobDetailOverviewUtils';
import { filterUiMatchRows, isApifyPipelineActive } from '@/shared/lib/jobDetailMatchesUtils';

const JOB_DETAIL_STAGE_BADGE = {
  SOURCED: 'bg-slate-100 text-slate-600',
  SCREENING: 'bg-blue-100 text-blue-700',
  ASSESSMENT_SENT: 'bg-purple-100 text-purple-700',
  ASSESSMENT_CLEARED: 'bg-violet-100 text-violet-700',
  INTERVIEW_1: 'bg-amber-100 text-amber-700',
  INTERVIEW_2: 'bg-amber-100 text-amber-700',
  INTERVIEW_3: 'bg-amber-100 text-amber-700',
  HR_ROUND: 'bg-orange-100 text-orange-700',
  OFFER: 'bg-emerald-100 text-emerald-700',
  JOINED: 'bg-slate-100 text-slate-700',
};

const NEXT_PIPELINE_STEP = {
  SOURCED: { next: 'SCREENING', label: 'Selected for next round' },
  SCREENING: { next: 'ASSESSMENT_SENT', label: 'Selected for next round' },
  ASSESSMENT_SENT: { next: 'ASSESSMENT_CLEARED', label: 'Mark Cleared' },
  ASSESSMENT_CLEARED: { next: 'OFFER', label: 'Select for Assessment Round' },
  INTERVIEW_1: { next: 'OFFER', label: 'Select for Assessment Round' },
  INTERVIEW_2: { next: 'OFFER', label: 'Select for Assessment Round' },
  INTERVIEW_3: { next: 'OFFER', label: 'Select for Assessment Round' },
  HR_ROUND: { next: 'OFFER', label: 'Select for Assessment Round' },
  OFFER: { next: 'JOINED', label: 'Mark Joined' },
};

function canAdvanceApplicationStage(app, perms) {
  if (perms.pipelineReadOnly || !perms.canAdvancePipeline) return false;
  const step = NEXT_PIPELINE_STEP[app?.stage];
  if (!step) return false;
  if (step.next === 'OFFER' || step.next === 'JOINED') return perms.canMoveToOffer;
  return true;
}

const JobDetailPage = () => {
  const { jobId } = useParams();
  const navigate = useNavigate();
  const { user } = useAuth();
  const perms = useHiringPermissions(user);
  const [searchParams] = useSearchParams();
  const tabParam = searchParams.get('tab');
  const [activeTab, setActiveTab] = useState(() =>
    ['overview', 'candidates', 'matches'].includes(tabParam) ? tabParam : 'overview'
  );
  const [job, setJob] = useState(null);
  const [applications, setApplications] = useState([]);
  const [matchingCandidates, setMatchingCandidates] = useState([]);
  const [loading, setLoading] = useState(true);
  const [matching, setMatching] = useState(false);
  const [apifyPipeline, setApifyPipeline] = useState(null);
  const [matchOrderMode, setMatchOrderMode] = useState('grid');
  const [demoGenerating, setDemoGenerating] = useState(false);
  const [stageUpdatingId, setStageUpdatingId] = useState(null);
  const { runWithClearanceCheck, clearanceDialog } = useAssessmentClearance();
  const autoScoredPipelineRef = useRef(null);

  const jobCandidateIds = useMemo(
    () => [...new Set(applications.map((a) => a.candidate_id).filter(Boolean))],
    [applications]
  );
  const {
    summaries: trajSummaries,
    loading: trajLoading,
    reload: reloadTrajSummaries,
  } = useCareerTrajectorySummaries(jobCandidateIds);

  useEffect(() => {
    fetchJobDetails();
  }, [jobId]);

  // Hydrate latest Apify pipeline on load so completed imports can auto-score.
  useEffect(() => {
    if (!jobId) return undefined;
    autoScoredPipelineRef.current = null;
    setMatchingCandidates([]);
    let cancelled = false;
    (async () => {
      try {
        const res = await jobsApi.getApifyLinkedInRun(jobId);
        if (!cancelled) setApifyPipeline(res.data?.pipeline || null);
      } catch {
        /* ignore */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [jobId]);

  useEffect(() => {
    if (tabParam && ['overview', 'candidates', 'matches'].includes(tabParam)) {
      setActiveTab(tabParam);
    }
  }, [tabParam]);

  const runLinkedInFirstMatch = async () => {
    setMatching(true);
    try {
      const matchRes = await jobsApi.match(jobId, { linkedin_first: true });
      setMatchingCandidates(filterUiMatchRows(matchRes.data.matches || []));
      setMatchOrderMode('linkedin_first');
      setActiveTab('matches');
      if (matchRes.data?.apify_pipeline) {
        setApifyPipeline(matchRes.data.apify_pipeline);
      }
    } finally {
      setMatching(false);
    }
  };

  useEffect(() => {
    const status = apifyPipeline?.status;
    if (!jobId || !status || !isApifyPipelineActive(apifyPipeline)) {
      return undefined;
    }
    const timer = setInterval(async () => {
      try {
        const res = await jobsApi.getApifyLinkedInRun(jobId);
        const pipeline = res.data?.pipeline || null;
        setApifyPipeline(pipeline);
        if (pipeline?.status === 'completed') {
          clearInterval(timer);
          toast.success(
            `LinkedIn search complete — ${pipeline.candidates_ingested || 0} profile(s) imported`
          );
          try {
            await runLinkedInFirstMatch();
          } catch {
            /* match errors toast below via silent fail — surface lightly */
            toast.message('Profiles imported. Click Find Matches to rank them.');
          }
        } else if (pipeline?.status === 'failed') {
          clearInterval(timer);
          toast.error(pipeline.error || 'LinkedIn Apify search failed');
        }
      } catch {
        /* ignore transient poll errors */
      }
    }, 12000);
    return () => clearInterval(timer);
  }, [jobId, apifyPipeline?.status, apifyPipeline?.id]);

  // If Apify already finished (e.g. email stage completed while poll was stopped), score once.
  useEffect(() => {
    if (!jobId || matching || matchingCandidates.length > 0) return undefined;
    if (apifyPipeline?.status !== 'completed') return undefined;
    if (!(apifyPipeline.candidates_ingested > 0)) return undefined;
    const pipeKey = `${apifyPipeline.id || 'na'}:${apifyPipeline.updated_at || apifyPipeline.candidates_ingested}`;
    if (autoScoredPipelineRef.current === pipeKey) return undefined;
    autoScoredPipelineRef.current = pipeKey;
    let cancelled = false;
    (async () => {
      try {
        if (!cancelled) await runLinkedInFirstMatch();
      } catch {
        /* user can click Find Matches */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [jobId, apifyPipeline?.status, apifyPipeline?.candidates_ingested, apifyPipeline?.id, apifyPipeline?.updated_at]);

  const fetchJobDetails = async () => {
    try {
      const [jobRes, appsRes] = await Promise.all([
        jobsApi.get(jobId),
        applicationsApi.list({ job_id: jobId }),
      ]);
      setJob(jobRes.data);
      setApplications(appsRes.data);
    } catch (error) {
      toast.error('Failed to fetch job details');
    } finally {
      setLoading(false);
    }
  };

  const _formatApiError = (error) => {
    const d = error?.response?.data?.detail;
    if (typeof d === 'string') return d;
    if (Array.isArray(d))
      return d.map((x) => (typeof x === 'object' && x?.msg ? x.msg : String(x))).join('; ');
    return error?.message || 'Request failed';
  };

  const displayMatches = useMemo(() => {
    if (matchOrderMode === 'linkedin_first') {
      return orderJobMatchesLinkedInFirst(matchingCandidates, { jobId });
    }
    return orderJobMatchesForGrid(matchingCandidates, { jobId });
  }, [matchingCandidates, jobId, matchOrderMode]);

  const setTab = (tab) => {
    setActiveTab(tab);
    const next = new URLSearchParams(searchParams);
    if (tab === 'overview') next.delete('tab');
    else next.set('tab', tab);
    navigate({ search: next.toString() ? `?${next.toString()}` : '' }, { replace: true });
  };

  const uiMatchCount = useMemo(
    () => filterUiMatchRows(matchingCandidates).length,
    [matchingCandidates]
  );

  const handleApifyLinkedInSearch = async () => {
    try {
      setMatchOrderMode('linkedin_first');
      setTab('matches');
      const res = await jobsApi.startApifyLinkedInSearch(jobId);
      setApifyPipeline(res.data?.pipeline || null);
      if (res.data?.started) {
        toast.info('LinkedIn search started via Apify');
      } else {
        toast.message(res.data?.message || 'Apify search not started');
      }
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to start LinkedIn search');
    }
  };

  const handleFindMatches = async () => {
    setMatching(true);
    setMatchOrderMode('grid');
    setTab('matches');
    try {
      const response = await jobsApi.match(jobId);
      const matches = filterUiMatchRows(response.data.matches || []);
      setMatchingCandidates(matches);
      setApifyPipeline(response.data.apify_pipeline || null);
      const ex = response.data.excel_count;
      const tp = response.data.talent_pool_count;
      const ai = response.data.ai_high_match_count;
      const li = response.data.linkedin_count;
      const pipeline = response.data.apify_pipeline;
      if (pipeline && isApifyPipelineActive(pipeline)) {
        toast.info('Searching LinkedIn via Apify — profiles will appear in a few minutes.');
      } else if (typeof ex === 'number' && typeof tp === 'number' && typeof ai === 'number') {
        const liPart = typeof li === 'number' ? `, ${li} LinkedIn` : '';
        toast.success(
          `Found ${matches.length} matches (${ex} Excel, ${tp} talent pool, ${ai} AI 90%+${liPart})`
        );
      } else {
        toast.success(`Found ${matches.length} matching candidates`);
      }
    } catch (error) {
      toast.error(`Failed to find matches: ${_formatApiError(error)}`);
    } finally {
      setMatching(false);
    }
  };

  const handleGenerateDemoCandidates = async () => {
    if (!job?.title || !job?.description || !(job?.skills?.length > 0)) return;
    setDemoGenerating(true);
    try {
      await jobsApi.generateDemoCandidates(jobId, 50);
      toast.success('Demo candidates generated. Finding matches...');
      await handleFindMatches();
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to generate demo candidates');
    } finally {
      setDemoGenerating(false);
    }
  };

  const handleAddToApplication = async (candidateId) => {
    try {
      await applicationsApi.create({
        job_id: jobId,
        candidate_id: candidateId,
        stage: 'SOURCED'
      });
      toast.success('Candidate added to pipeline');
      fetchJobDetails();
      setMatchingCandidates(matchingCandidates.filter(m => m.candidate.id !== candidateId));
    } catch (error) {
      toast.error(error.response?.data?.detail || 'Failed to add candidate');
    }
  };

  const advanceApplicationStage = async (app) => {
    const step = NEXT_PIPELINE_STEP[app?.stage];
    if (!step?.next || !app?.id) return;

    const doUpdate = async (reasonOverride) => {
      setStageUpdatingId(app.id);
      try {
        await applicationsApi.updateStage(app.id, {
          stage: step.next,
          ...(reasonOverride ? { reason: reasonOverride } : {}),
        });
        toast.success('Updated');
        await fetchJobDetails();
      } catch (error) {
        toast.error(error.response?.data?.detail || 'Failed to update stage');
      } finally {
        setStageUpdatingId(null);
      }
    };

    await runWithClearanceCheck(app, app.stage, step.next, doUpdate);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-[60vh]">
        <Loader2 className="w-8 h-8 animate-spin text-indigo-600" />
      </div>
    );
  }

  if (!job) {
    return (
      <div className="text-center py-12">
        <p className="text-slate-500">Job not found</p>
        <Link to="/jobs">
          <Button variant="outline" className="mt-4">Back to Jobs</Button>
        </Link>
      </div>
    );
  }

  const matchesEmpty = uiMatchCount === 0;

  return (
    <div className="job-detail-root" data-testid="job-detail-root">
      <Link to="/jobs" className="jd-back" data-testid="back-btn">
        ← Back to Jobs
      </Link>

      <div className="jd-hero">
        <div className="jd-job-icon" aria-hidden>
          ▣
        </div>
        <div>
          <div className="jd-title-row">
            <h1 data-testid="job-detail-title">{job.title}</h1>
            <span className={`jd-badge ${statusBadgeClass(job.status)}`}>{job.status}</span>
          </div>
          {job.normalized_title ? (
            <div className="jd-ai-line">✣ AI: {job.normalized_title}</div>
          ) : null}
          <div className="jd-meta">
            {job.location ? <span>⌖ {job.location}</span> : null}
            {job.work_mode ? <span className="jd-pill">{job.work_mode}</span> : null}
            {job.seniority ? <span className="jd-pill">{job.seniority}</span> : null}
            {job.business_department ? <span className="jd-pill">{job.business_department}</span> : null}
          </div>
        </div>
        <div className="jd-actions">
          <Link to={`/pipeline?job=${job.id}`} className="jd-action">
            ♙ Pipeline ({applications.length})
          </Link>
          {uiMatchCount === 0 ? (
            <button
              type="button"
              className="jd-action jd-action-outline"
              onClick={handleGenerateDemoCandidates}
              disabled={demoGenerating || matching || !(job?.title && job?.description && job?.skills?.length > 0)}
              data-testid="play-demo-btn"
            >
              {demoGenerating ? 'Generating…' : '▷ Play Demo (Generate 50)'}
            </button>
          ) : null}
          <button
            type="button"
            className="jd-action jd-action-blue"
            onClick={handleApifyLinkedInSearch}
            disabled={
              matching || isApifyPipelineActive(apifyPipeline)
            }
            data-testid="search-linkedin-apify-btn"
          >
            Search LinkedIn
          </button>
          <button
            type="button"
            className="jd-action jd-action-primary"
            onClick={handleFindMatches}
            disabled={matching}
            data-testid="find-matches-btn"
          >
            {matching ? 'Finding…' : '◎ Find Matches'}
          </button>
        </div>
      </div>

      <nav className="jd-tabs" role="tablist" aria-label="Job sections">
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === 'overview'}
          className={`jd-tab ${activeTab === 'overview' ? 'active' : ''}`}
          onClick={() => setTab('overview')}
        >
          Overview
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === 'candidates'}
          className={`jd-tab ${activeTab === 'candidates' ? 'active' : ''}`}
          onClick={() => setTab('candidates')}
        >
          Candidates <b>{applications.length}</b>
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === 'matches'}
          className={`jd-tab ${activeTab === 'matches' ? 'active' : ''}`}
          onClick={() => setTab('matches')}
        >
          AI Matches <b>{uiMatchCount}</b>
        </button>
      </nav>

      {activeTab === 'overview' ? (
        <JobDetailOverviewTab job={job} applications={applications} matchingCandidates={matchingCandidates} />
      ) : null}

      {activeTab === 'candidates' ? (
        <div className="jd-tab-panel">
          <JobDetailCandidatesTab
            applications={applications}
            jobId={jobId}
            trajSummaries={trajSummaries}
            trajLoading={trajLoading}
            reloadTrajSummaries={reloadTrajSummaries}
            perms={perms}
            stageUpdatingId={stageUpdatingId}
            advanceApplicationStage={advanceApplicationStage}
            canAdvanceApplicationStage={canAdvanceApplicationStage}
            nextPipelineStep={NEXT_PIPELINE_STEP}
            onFindMatches={handleFindMatches}
          />
        </div>
      ) : null}

      {activeTab === 'matches' ? (
        <div
          className={`jd-tab-panel ${
            matchesEmpty ? 'jd-tab-panel--matches-empty' : 'jd-tab-panel--matches-found'
          }`}
        >
          <JobDetailMatchesTab
            job={job}
            matching={matching}
            matchingCandidates={matchingCandidates}
            displayMatches={displayMatches}
            apifyPipeline={apifyPipeline}
            matchOrderMode={matchOrderMode}
            onFindMatches={handleFindMatches}
            onAddToPipeline={handleAddToApplication}
            onGenerateDemo={handleGenerateDemoCandidates}
            demoGenerating={demoGenerating}
          />
        </div>
      ) : null}
      {clearanceDialog}
    </div>
  );
};

export default JobDetailPage;
