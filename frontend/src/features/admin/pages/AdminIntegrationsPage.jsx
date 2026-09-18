import React, { useEffect, useMemo, useState } from 'react';
import { Loader2 } from 'lucide-react';
import { toast } from 'sonner';
import { adminApi, jobsApi } from '@/shared/lib/api';
import { SMART_HIRING_ONLY } from '@/shared/config/appModules';
import AdminIntegrationsOrgFilterBar from '@/features/admin/components/admin/integrations/AdminIntegrationsOrgFilterBar';
import {
  CONNECTOR_KEYS,
  CONNECTOR_META,
  computeReadiness,
  connectorMetaLine,
  connectorStatus,
  enabledStatusLabel,
  numOrUndef,
  parseScopesToString,
} from '@/shared/lib/adminIntegrationsUtils';

const TABS = [
  { id: 'overview', label: 'Overview' },
  { id: 'company-db', label: 'Company DB' },
  { id: 'linkedin', label: 'LinkedIn' },
  { id: 'naukri', label: 'Naukri' },
  { id: 'monster', label: 'Monster' },
  { id: 'audit', label: 'Audit Log' },
];

const emptyConnector = {
  enabled: false,
  mongo_url: '',
  db_name: '',
  collection_name: '',
  client_id: '',
  client_secret: '',
  base_url: '',
  scopes: '',
};

function StatusPill({ tone, label }) {
  return <span className={`aic-status-pill aic-status-pill--${tone}`}>{label}</span>;
}

const AdminIntegrationsPage = () => {
  const [loading, setLoading] = useState(true);
  const [configs, setConfigs] = useState({});
  const [saving, setSaving] = useState(false);
  const [health, setHealth] = useState(null);
  const [linkedInStatus, setLinkedInStatus] = useState(null);
  const [linkedInTesting, setLinkedInTesting] = useState(false);
  const [manualRequestId, setManualRequestId] = useState('');
  const [manualFetching, setManualFetching] = useState(false);
  const [exportQueue, setExportQueue] = useState([]);
  const [queueLoading, setQueueLoading] = useState(false);
  const [syncingJobs, setSyncingJobs] = useState(false);
  const [apifyStatus, setApifyStatus] = useState(null);
  const [apifyTesting, setApifyTesting] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');
  const [openJobs, setOpenJobs] = useState([]);

  const linkedInApifyMode = (configs.LINKEDIN?.api_mode || 'apify') === 'apify';

  const form = useMemo(() => {
    const get = (key) => configs?.[key] || {};
    return {
      COMPANY_DB_CANDIDATES: {
        ...emptyConnector,
        enabled: !!get('COMPANY_DB_CANDIDATES')?.enabled || false,
        mongo_url: get('COMPANY_DB_CANDIDATES')?.mongo_url || '',
        db_name: get('COMPANY_DB_CANDIDATES')?.db_name || '',
        collection_name: get('COMPANY_DB_CANDIDATES')?.collection_name || '',
      },
      LINKEDIN: {
        ...emptyConnector,
        enabled: !!get('LINKEDIN')?.enabled || false,
        mongo_url: get('LINKEDIN')?.mongo_url || '',
        db_name: get('LINKEDIN')?.db_name || '',
        collection_name: get('LINKEDIN')?.collection_name || '',
        client_id: get('LINKEDIN')?.client_id || '',
        client_secret: get('LINKEDIN')?.client_secret || '',
        base_url: get('LINKEDIN')?.base_url || '',
        scopes: parseScopesToString(get('LINKEDIN')?.scopes),
      },
      NAUKRI: {
        ...emptyConnector,
        enabled: !!get('NAUKRI')?.enabled || false,
        mongo_url: get('NAUKRI')?.mongo_url || '',
        db_name: get('NAUKRI')?.db_name || '',
        collection_name: get('NAUKRI')?.collection_name || '',
        client_id: get('NAUKRI')?.client_id || '',
        client_secret: get('NAUKRI')?.client_secret || '',
        base_url: get('NAUKRI')?.base_url || '',
        scopes: parseScopesToString(get('NAUKRI')?.scopes),
      },
      MONSTER: {
        ...emptyConnector,
        enabled: !!get('MONSTER')?.enabled || false,
        mongo_url: get('MONSTER')?.mongo_url || '',
        db_name: get('MONSTER')?.db_name || '',
        collection_name: get('MONSTER')?.collection_name || '',
        client_id: get('MONSTER')?.client_id || '',
        client_secret: get('MONSTER')?.client_secret || '',
        base_url: get('MONSTER')?.base_url || '',
        scopes: parseScopesToString(get('MONSTER')?.scopes),
      },
    };
  }, [configs]);

  const readiness = useMemo(
    () => computeReadiness(configs, health, linkedInStatus),
    [configs, health, linkedInStatus]
  );

  useEffect(() => {
    const load = async () => {
      try {
        const res = await adminApi.getConnectorConfigs();
        setConfigs(res.data || {});
      } catch {
        toast.error('Failed to load connector configs');
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  useEffect(() => {
    let alive = true;
    (async () => {
      try {
        const res = await jobsApi.list('OPEN');
        if (!alive) return;
        setOpenJobs(Array.isArray(res.data) ? res.data : []);
      } catch {
        if (!alive) return;
        setOpenJobs([]);
      }
    })();
    return () => {
      alive = false;
    };
  }, []);

  const refreshApifyStatus = async () => {
    try {
      const res = await adminApi.getApifyLinkedInStatus();
      setApifyStatus(res.data || null);
    } catch {
      setApifyStatus(null);
    }
  };

  const handleApifyTest = async () => {
    setApifyTesting(true);
    try {
      const res = await adminApi.testApifyLinkedIn();
      if (res.data?.ok) {
        toast.success(res.data.message || 'Apify connection OK');
      } else {
        toast.error(res.data?.message || 'Apify test failed');
      }
      await refreshApifyStatus();
      const cfgRes = await adminApi.getConnectorConfigs();
      setConfigs(cfgRes.data || {});
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Apify connection test failed');
    } finally {
      setApifyTesting(false);
    }
  };

  const refreshLinkedInStatus = async () => {
    try {
      const res = await adminApi.getLinkedInStatus();
      setLinkedInStatus(res.data || null);
    } catch {
      setLinkedInStatus(null);
    }
  };

  const refreshLinkedInExportQueue = async () => {
    setQueueLoading(true);
    try {
      const res = await adminApi.getLinkedInExportQueue(25);
      setExportQueue(res.data?.items || []);
    } catch {
      setExportQueue([]);
    } finally {
      setQueueLoading(false);
    }
  };

  useEffect(() => {
    if (loading) return;
    (async () => {
      try {
        const res = await adminApi.getConnectorsHealth();
        setHealth(res.data);
      } catch {
        setHealth(null);
      }
      await refreshLinkedInStatus();
      await refreshLinkedInExportQueue();
      if ((configs.LINKEDIN?.api_mode || 'apify') === 'apify') {
        await refreshApifyStatus();
      }
    })();
  }, [loading, configs]);

  const handleLinkedInTest = async () => {
    setLinkedInTesting(true);
    try {
      const res = await adminApi.testLinkedInConnection();
      if (res.data?.ok) {
        toast.success(res.data.message || 'LinkedIn API connected');
      } else {
        toast.error(res.data?.message || 'LinkedIn test failed');
      }
      await refreshLinkedInStatus();
      const cfgRes = await adminApi.getConnectorConfigs();
      setConfigs(cfgRes.data || {});
      const healthRes = await adminApi.getConnectorsHealth();
      setHealth(healthRes.data);
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'LinkedIn connection test failed');
    } finally {
      setLinkedInTesting(false);
    }
  };

  const handleManualLinkedInFetch = async () => {
    const rid = manualRequestId.trim();
    if (!rid) {
      toast.error('Enter a LinkedIn export request ID');
      return;
    }
    setManualFetching(true);
    try {
      const res = await adminApi.fetchLinkedInExport(rid);
      const upserted = res.data?.upserted ?? 0;
      toast.success(
        upserted > 0
          ? `Imported ${upserted} candidate(s) from LinkedIn into the talent pool`
          : `LinkedIn returned ${res.data?.elements ?? 0} profile(s); none were saved (check mapping)`
      );
      setManualRequestId('');
      await refreshLinkedInStatus();
      await refreshLinkedInExportQueue();
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'Failed to fetch export from LinkedIn');
    } finally {
      setManualFetching(false);
    }
  };

  const handleSyncLinkedInJobs = async () => {
    setSyncingJobs(true);
    try {
      const res = await adminApi.syncLinkedInOpenJobs(50);
      const synced = res.data?.synced ?? 0;
      const failed = res.data?.failed ?? 0;
      toast.success(`LinkedIn job sync: ${synced} synced, ${failed} failed`);
      await refreshLinkedInStatus();
    } catch (e) {
      toast.error(e?.response?.data?.detail || 'LinkedIn job sync failed');
    } finally {
      setSyncingJobs(false);
    }
  };

  const pendingExportIdsText = useMemo(() => {
    const raw = configs.LINKEDIN?.pending_export_request_ids;
    if (!raw) return '';
    if (Array.isArray(raw)) return raw.join('\n');
    return String(raw);
  }, [configs.LINKEDIN?.pending_export_request_ids]);

  const updateField = (key, field, value) => {
    setConfigs((prev) => ({
      ...prev,
      [key]: {
        ...(prev?.[key] || {}),
        [field]: value,
      },
    }));
  };

  const handleSave = async () => {
    setSaving(true);
    try {
      const payloads = [
        {
          name: 'COMPANY_DB_CANDIDATES',
          data: {
            enabled: !!form.COMPANY_DB_CANDIDATES.enabled,
            mongo_url: form.COMPANY_DB_CANDIDATES.mongo_url || null,
            db_name: form.COMPANY_DB_CANDIDATES.db_name || null,
            collection_name: form.COMPANY_DB_CANDIDATES.collection_name || null,
          },
        },
        {
          name: 'LINKEDIN',
          data: {
            enabled: !!form.LINKEDIN.enabled,
            api_mode: configs.LINKEDIN?.api_mode || 'apify',
            apify_search_actor_id:
              configs.LINKEDIN?.apify_search_actor_id || 'harvestapi/linkedin-profile-search',
            apify_enrich_actor_id:
              configs.LINKEDIN?.apify_enrich_actor_id || 'dev_fusion/linkedin-profile-scraper',
            apify_email_actor_id:
              configs.LINKEDIN?.apify_email_actor_id || 'khadinakbar/linkedin-profile-email-scraper',
            apify_max_results_per_search: numOrUndef(configs.LINKEDIN?.apify_max_results_per_search) ?? 30,
            apify_enrich_batch_size: numOrUndef(configs.LINKEDIN?.apify_enrich_batch_size) ?? 30,
            apify_default_geocode: configs.LINKEDIN?.apify_default_geocode || 'in:0:0:0:0:0:0',
            apify_email_fallback_enabled: !!configs.LINKEDIN?.apify_email_fallback_enabled,
            client_id: form.LINKEDIN.client_id || null,
            client_secret: (form.LINKEDIN.client_secret || '').trim() || undefined,
            linkedin_organization_id:
              (configs.LINKEDIN?.linkedin_organization_id || '').trim() || null,
            linkedin_api_version: configs.LINKEDIN?.linkedin_api_version || '202603',
            base_url: configs.LINKEDIN?.base_url || 'https://api.linkedin.com/rest',
            oauth_token_url:
              configs.LINKEDIN?.oauth_token_url || 'https://www.linkedin.com/oauth/v2/accessToken',
            webhook_secret: (configs.LINKEDIN?.webhook_secret || '').trim() || undefined,
            pending_export_request_ids: configs.LINKEDIN?.pending_export_request_ids || undefined,
            linkedin_company_name:
              (configs.LINKEDIN?.linkedin_company_name || '').trim() || undefined,
          },
        },
        {
          name: 'NAUKRI',
          data: {
            enabled: !!form.NAUKRI.enabled,
            mongo_url: form.NAUKRI.mongo_url || null,
            db_name: form.NAUKRI.db_name || null,
            collection_name: form.NAUKRI.collection_name || null,
            client_id: form.NAUKRI.client_id || null,
            client_secret: (form.NAUKRI.client_secret || '').trim() || undefined,
            base_url: form.NAUKRI.base_url || null,
            scopes: form.NAUKRI.scopes || undefined,
            oauth_token_url: configs.NAUKRI?.oauth_token_url || undefined,
            refresh_token: (configs.NAUKRI?.refresh_token || '').trim() || undefined,
            page_size: numOrUndef(configs.NAUKRI?.page_size),
            max_retries: numOrUndef(configs.NAUKRI?.max_retries),
            min_interval_ms: numOrUndef(configs.NAUKRI?.min_interval_ms),
          },
        },
        {
          name: 'MONSTER',
          data: {
            enabled: !!form.MONSTER.enabled,
            mongo_url: form.MONSTER.mongo_url || null,
            db_name: form.MONSTER.db_name || null,
            collection_name: form.MONSTER.collection_name || null,
            client_id: form.MONSTER.client_id || null,
            client_secret: (form.MONSTER.client_secret || '').trim() || undefined,
            base_url: form.MONSTER.base_url || null,
            scopes: form.MONSTER.scopes || undefined,
            oauth_token_url: configs.MONSTER?.oauth_token_url || undefined,
            refresh_token: (configs.MONSTER?.refresh_token || '').trim() || undefined,
            page_size: numOrUndef(configs.MONSTER?.page_size),
            max_retries: numOrUndef(configs.MONSTER?.max_retries),
            min_interval_ms: numOrUndef(configs.MONSTER?.min_interval_ms),
          },
        },
      ];

      for (const p of payloads) {
        await adminApi.updateConnectorConfig(p.name, p.data);
      }

      toast.success('Connector configs saved');
      const res = await adminApi.getConnectorConfigs();
      setConfigs(res.data || {});
    } catch (e) {
      toast.error('Failed to save connector configs');
      console.error(e);
    } finally {
      setSaving(false);
    }
  };

  const renderSaveButton = (testId) => (
    <button
      type="button"
      className="aic-btn aic-btn-primary"
      onClick={handleSave}
      disabled={saving || loading}
      data-testid={testId}
    >
      {saving ? <Loader2 className="aic-btn-spinner" aria-hidden /> : null}
      {saving ? 'Saving...' : 'Save Connector Configs'}
    </button>
  );

  if (loading) {
    return (
      <div
        className="hiring-dashboard-root top-operational aic-root"
        data-testid="admin-integrations-command-root"
      >
        <div className="aic-loading">
          <Loader2 className="aic-loading-spinner" aria-hidden />
        </div>
      </div>
    );
  }

  return (
    <div
      className="hiring-dashboard-root top-operational aic-root"
      data-testid="admin-integrations-command-root"
    >
      {SMART_HIRING_ONLY ? (
        <header className="aic-topbar" aria-label="Admin integrations filters">
          <div className="aic-topbar-left">
            <h3 className="aic-topbar-title">Settings &amp; Connectors</h3>
            <AdminIntegrationsOrgFilterBar jobs={openJobs} />
          </div>
        </header>
      ) : null}

      <div className="aic-content">
        <div className="aic-page-head">
          <div>
            <div className="aic-eyebrow">Admin</div>
            <h1>Admin Integrations</h1>
            <p>
              Configure candidate-source connectors, official job-board credentials, webhook ingestion,
              and external candidate database settings.
            </p>
          </div>
          {renderSaveButton('admin-integrations-save-header')}
        </div>

        <section className="aic-health-grid" aria-label="Connector health and readiness">
          <div className="aic-card">
            <div className="aic-section-title">
              <div>
                <h2>Connector health</h2>
                <p>Last HTTP / Mongo fetch status and readiness summary.</p>
              </div>
              <span className="aic-chip">{CONNECTOR_KEYS.length} connectors</span>
            </div>
            <div className="aic-health-cards">
              {CONNECTOR_KEYS.map((key) => {
                const healthEntry = health?.[key];
                const configEntry = configs?.[key];
                const status = connectorStatus(key, healthEntry, configEntry, linkedInStatus);
                const meta = connectorMetaLine(key, healthEntry, configEntry, linkedInStatus);
                return (
                  <div key={key} className="aic-connector-card">
                    <div>
                      <h4>{CONNECTOR_META[key]?.title || key}</h4>
                      <div className="aic-mini-meta">
                        {meta.line1}
                        <br />
                        {meta.line2}
                        <br />
                        {meta.line3}
                      </div>
                    </div>
                    <StatusPill tone={status.tone} label={status.label} />
                  </div>
                );
              })}
            </div>
          </div>

          <div className="aic-card">
            <div className="aic-section-title">
              <div>
                <h2>Integration readiness</h2>
                <p>Quick operational checklist before enabling production sync.</p>
              </div>
            </div>
            <div className="aic-summary">
              <div className="aic-metric">
                <span>Enabled sources</span>
                <b>
                  {readiness.enabledCount}/{readiness.totalConnectors}
                </b>
              </div>
              <div className="aic-metric">
                <span>Pending exports</span>
                <b>{readiness.pendingExports}</b>
              </div>
              <div className="aic-metric">
                <span>Secrets missing</span>
                <b>{readiness.secretsMissing}</b>
              </div>
              <div className="aic-metric">
                <span>Webhook status</span>
                <b>{readiness.webhookStatus}</b>
              </div>
            </div>
            <div className="aic-info">
              Recommended sequence: save credentials → test connection → register webhook → sync open
              jobs → enable scheduled ingestion.
            </div>
            <div className="aic-compact-grid">
              <span className="aic-chip">Secrets encrypted</span>
              <span className="aic-chip">Retry policy ready</span>
              <span className="aic-chip">Audit trail enabled</span>
            </div>
          </div>
        </section>

        <div className="aic-tabs" role="tablist" aria-label="Integration sections">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              type="button"
              role="tab"
              aria-selected={activeTab === tab.id}
              className={`aic-tab${activeTab === tab.id ? ' aic-tab--active' : ''}`}
              onClick={() => setActiveTab(tab.id)}
              data-testid={`admin-integrations-tab-${tab.id}`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {activeTab === 'company-db' && (
          <section className="aic-card aic-connector-form" data-testid="admin-integrations-company-db">
            <div className="aic-section-title">
              <div>
                <h2>Company Candidates Source</h2>
                <p>Provide the external Mongo location/path where company candidates live.</p>
              </div>
              <StatusPill {...enabledStatusLabel(!!configs.COMPANY_DB_CANDIDATES?.enabled)} />
            </div>
            <label className="aic-chip aic-chip-check">
              <input
                type="checkbox"
                checked={!!configs.COMPANY_DB_CANDIDATES?.enabled}
                onChange={(e) => updateField('COMPANY_DB_CANDIDATES', 'enabled', e.target.checked)}
              />
              Enable external company DB
            </label>
            <div className="aic-form-row aic-form-row-spaced">
              <div className="aic-field">
                <label htmlFor="company-db-mongo-url">Mongo URL</label>
                <input
                  id="company-db-mongo-url"
                  value={configs.COMPANY_DB_CANDIDATES?.mongo_url || ''}
                  onChange={(e) => updateField('COMPANY_DB_CANDIDATES', 'mongo_url', e.target.value)}
                  placeholder="mongodb://host:27017"
                />
              </div>
              <div className="aic-field">
                <label htmlFor="company-db-name">DB Name</label>
                <input
                  id="company-db-name"
                  value={configs.COMPANY_DB_CANDIDATES?.db_name || ''}
                  onChange={(e) => updateField('COMPANY_DB_CANDIDATES', 'db_name', e.target.value)}
                  placeholder="aai_hrms"
                />
              </div>
            </div>
            <div className="aic-field">
              <label htmlFor="company-db-collection">Collection Name</label>
              <input
                id="company-db-collection"
                value={configs.COMPANY_DB_CANDIDATES?.collection_name || ''}
                onChange={(e) => updateField('COMPANY_DB_CANDIDATES', 'collection_name', e.target.value)}
                placeholder="candidates"
              />
            </div>
          </section>
        )}

        {activeTab === 'linkedin' && (
          <section className="aic-card aic-connector-form" data-testid="admin-integrations-linkedin">
            <div className="aic-section-title">
              <div>
                <h2>
                  <span className="aic-iconbox">in</span>
                  LinkedIn sourcing
                  {!linkedInApifyMode ? (
                    <small className="aic-subtitle"> (Recruiter System Connect)</small>
                  ) : null}
                </h2>
                <p>
                  {linkedInApifyMode
                    ? 'Discover candidates on LinkedIn via Apify (HarvestAPI search) when you run Find Matches on a job. Search uses job title and location; full profiles are imported directly.'
                    : 'LinkedIn Talent API (Recruiter System Connect): candidates import when recruiters export profiles via webhook.'}
                </p>
              </div>
              <StatusPill {...enabledStatusLabel(!!configs.LINKEDIN?.enabled)} />
            </div>

            {!linkedInApifyMode ? (
              <div className="aic-note">
                <b>No proactive LinkedIn search (RSC mode).</b> RSC only delivers profiles when a
                recruiter uses one-click export in LinkedIn Recruiter.
              </div>
            ) : apifyStatus == null ? (
              <div className="aic-info" data-testid="apify-status-loading-banner">
                <b>Apify mode — proactive people search.</b> Checking APIFY_API_TOKEN and connector
                status…
              </div>
            ) : apifyStatus.token_set ? (
              <div className="aic-success" data-testid="apify-ready-banner">
                <b>Apify mode — proactive people search.</b> APIFY_API_TOKEN is configured.
                Find Matches starts a search + enrich pipeline automatically when fewer than 10
                LinkedIn profiles exist for the job.
              </div>
            ) : (
              <div className="aic-note" data-testid="apify-token-missing-banner">
                <b>Apify mode — proactive people search.</b> Set{' '}
                <code>APIFY_API_TOKEN</code> in the server environment (for Docker: root{' '}
                <code>.env</code>, then recreate the <code>api</code> container). Find Matches
                starts a search + enrich pipeline automatically when fewer than 10 LinkedIn
                profiles exist for the job.
              </div>
            )}

            <div className="aic-field aic-field-inline">
              <label htmlFor="linkedin-api-mode">Integration mode</label>
              <select
                id="linkedin-api-mode"
                value={configs.LINKEDIN?.api_mode || 'apify'}
                onChange={(e) => updateField('LINKEDIN', 'api_mode', e.target.value)}
              >
                <option value="apify">Apify (search + enrich)</option>
                <option value="talent_rsc">LinkedIn RSC (recruiter export)</option>
              </select>
            </div>

            <div className="aic-queue">
              <label className="aic-chip aic-chip-check">
                <input
                  type="checkbox"
                  checked={!!configs.LINKEDIN?.enabled}
                  onChange={(e) => updateField('LINKEDIN', 'enabled', e.target.checked)}
                />
                Enable LinkedIn connector
              </label>
              {linkedInApifyMode ? (
                <button
                  type="button"
                  className="aic-btn aic-btn-secondary"
                  disabled={apifyTesting}
                  onClick={handleApifyTest}
                >
                  {apifyTesting ? 'Testing…' : 'Test Apify connection'}
                </button>
              ) : (
                <>
                  <button
                    type="button"
                    className="aic-btn aic-btn-secondary"
                    disabled={linkedInTesting}
                    onClick={handleLinkedInTest}
                  >
                    {linkedInTesting ? 'Testing…' : 'Test connection'}
                  </button>
                  <button
                    type="button"
                    className="aic-btn aic-btn-ghost"
                    disabled={syncingJobs || !configs.LINKEDIN?.enabled}
                    onClick={handleSyncLinkedInJobs}
                  >
                    {syncingJobs ? 'Syncing jobs…' : 'Sync OPEN jobs to LinkedIn'}
                  </button>
                </>
              )}
              <div className="aic-queue-box">
                {linkedInApifyMode && apifyStatus ? (
                  <>
                    Apify:{' '}
                    {apifyStatus.configured ? 'Configured' : apifyStatus.configuration_message}
                    {' · '}
                    Token in environment: {apifyStatus.token_set ? 'Yes' : 'No'}
                  </>
                ) : linkedInStatus ? (
                  <>
                    Status:{' '}
                    {linkedInStatus.configured
                      ? 'Configured'
                      : linkedInStatus.configuration_message}{' '}
                    · Pending exports in queue: {linkedInStatus.pending_export_count ?? 0}
                  </>
                ) : (
                  'Status: —'
                )}
              </div>
            </div>

            {linkedInApifyMode ? (
              <>
                <div className="aic-form-row">
                  <div className="aic-field aic-field-wide">
                    <label htmlFor="linkedin-search-actor">Search actor ID</label>
                    <input
                      id="linkedin-search-actor"
                      value={configs.LINKEDIN?.apify_search_actor_id || 'harvestapi/linkedin-profile-search'}
                      onChange={(e) => updateField('LINKEDIN', 'apify_search_actor_id', e.target.value)}
                    />
                  </div>
                  <div className="aic-field aic-field-wide">
                    <label htmlFor="linkedin-enrich-actor">Enrich actor ID</label>
                    <input
                      id="linkedin-enrich-actor"
                      value={configs.LINKEDIN?.apify_enrich_actor_id || 'dev_fusion/linkedin-profile-scraper'}
                      onChange={(e) => updateField('LINKEDIN', 'apify_enrich_actor_id', e.target.value)}
                    />
                  </div>
                </div>
                <div className="aic-form-row">
                  <div className="aic-field">
                    <label htmlFor="linkedin-max-results">Max results per search</label>
                    <input
                      id="linkedin-max-results"
                      type="number"
                      min={5}
                      max={100}
                      value={configs.LINKEDIN?.apify_max_results_per_search ?? 30}
                      onChange={(e) =>
                        updateField('LINKEDIN', 'apify_max_results_per_search', e.target.value)
                      }
                    />
                  </div>
                  <div className="aic-field">
                    <label htmlFor="linkedin-enrich-batch">Enrich batch size</label>
                    <input
                      id="linkedin-enrich-batch"
                      type="number"
                      min={5}
                      max={100}
                      value={configs.LINKEDIN?.apify_enrich_batch_size ?? 30}
                      onChange={(e) => updateField('LINKEDIN', 'apify_enrich_batch_size', e.target.value)}
                    />
                  </div>
                </div>
                <div className="aic-field">
                  <label htmlFor="linkedin-geocode">Default geocode location</label>
                  <input
                    id="linkedin-geocode"
                    value={configs.LINKEDIN?.apify_default_geocode || 'in:0:0:0:0:0:0'}
                    onChange={(e) => updateField('LINKEDIN', 'apify_default_geocode', e.target.value)}
                    placeholder="in:0:0:0:0:0:0"
                  />
                </div>
                <label className="aic-chip aic-chip-check">
                  <input
                    type="checkbox"
                    checked={!!configs.LINKEDIN?.apify_email_fallback_enabled}
                    onChange={(e) =>
                      updateField('LINKEDIN', 'apify_email_fallback_enabled', e.target.checked)
                    }
                  />
                  Enable optional email fallback actor (khadinakbar) for profiles without email
                </label>
                <p className="aic-hint" style={{ marginTop: 4, opacity: 0.8, fontSize: 12 }}>
                  HarvestAPI search always uses Full + email search. When this is on, profiles that
                  still lack a real email are re-checked via the email actor.
                </p>
              </>
            ) : (
              <>
                <div className="aic-form-row">
                  <div className="aic-field">
                    <label htmlFor="linkedin-client-id">Client ID *</label>
                    <input
                      id="linkedin-client-id"
                      value={configs.LINKEDIN?.client_id || ''}
                      onChange={(e) => updateField('LINKEDIN', 'client_id', e.target.value)}
                      placeholder="From LinkedIn Developer Portal"
                    />
                  </div>
                  <div className="aic-field">
                    <label htmlFor="linkedin-client-secret">Client Secret *</label>
                    <input
                      id="linkedin-client-secret"
                      type="password"
                      value={configs.LINKEDIN?.client_secret || ''}
                      onChange={(e) => updateField('LINKEDIN', 'client_secret', e.target.value)}
                      placeholder={
                        configs.LINKEDIN?.client_secret_set ? 'Saved (leave blank to keep)' : 'Paste secret'
                      }
                    />
                  </div>
                </div>
                <div className="aic-form-row">
                  <div className="aic-field">
                    <label htmlFor="linkedin-org-id">Organization ID *</label>
                    <input
                      id="linkedin-org-id"
                      value={configs.LINKEDIN?.linkedin_organization_id || ''}
                      onChange={(e) => updateField('LINKEDIN', 'linkedin_organization_id', e.target.value)}
                      placeholder="e.g. 12345 (urn:li:organization:12345)"
                    />
                  </div>
                  <div className="aic-field">
                    <label htmlFor="linkedin-company-name">Company name (job sync)</label>
                    <input
                      id="linkedin-company-name"
                      value={configs.LINKEDIN?.linkedin_company_name || ''}
                      onChange={(e) => updateField('LINKEDIN', 'linkedin_company_name', e.target.value)}
                      placeholder="Shown on LinkedIn simpleJobPostings sync"
                    />
                  </div>
                </div>
                <div className="aic-form-row">
                  <div className="aic-field">
                    <label htmlFor="linkedin-api-version">API version</label>
                    <input
                      id="linkedin-api-version"
                      value={configs.LINKEDIN?.linkedin_api_version || '202603'}
                      onChange={(e) => updateField('LINKEDIN', 'linkedin_api_version', e.target.value)}
                      placeholder="202603"
                    />
                  </div>
                  <div className="aic-field">
                    <label htmlFor="linkedin-webhook-secret">Webhook secret (optional, legacy)</label>
                    <input
                      id="linkedin-webhook-secret"
                      type="password"
                      value={configs.LINKEDIN?.webhook_secret || ''}
                      onChange={(e) => updateField('LINKEDIN', 'webhook_secret', e.target.value)}
                      placeholder="Custom X-LinkedIn-Signature only"
                    />
                  </div>
                </div>
                <div className="aic-field">
                  <label htmlFor="linkedin-webhook-url">Webhook URL (register in LinkedIn RSC push notifications)</label>
                  <input
                    id="linkedin-webhook-url"
                    readOnly
                    value={linkedInStatus?.webhook_url || ''}
                    className="aic-input-mono"
                  />
                  <p className="aic-field-hint">
                    LinkedIn validates this URL with an HTTP GET and challengeCode query param before
                    enabling push events.
                  </p>
                </div>
                <div className="aic-field">
                  <label htmlFor="linkedin-pending-exports">Pending export request IDs (recovery)</label>
                  <textarea
                    id="linkedin-pending-exports"
                    rows={3}
                    className="aic-input-mono"
                    value={pendingExportIdsText}
                    onChange={(e) =>
                      updateField(
                        'LINKEDIN',
                        'pending_export_request_ids',
                        e.target.value
                          .split(/[\n,]+/)
                          .map((s) => s.trim())
                          .filter(Boolean)
                      )
                    }
                    placeholder="One requestId per line (24h validity)"
                  />
                </div>

                <div className="aic-divider" />
                <p className="aic-section-label">Export queue</p>
                <div className="aic-queue">
                  <button
                    type="button"
                    className="aic-btn aic-btn-secondary"
                    disabled={queueLoading}
                    onClick={refreshLinkedInExportQueue}
                  >
                    {queueLoading ? 'Loading…' : 'Refresh queue'}
                  </button>
                  <div className="aic-queue-box">
                    {exportQueue.length > 0 ? (
                      <table className="aic-queue-table">
                        <thead>
                          <tr>
                            <th>Request ID</th>
                            <th>Status</th>
                            <th>Upserted</th>
                            <th>Updated</th>
                          </tr>
                        </thead>
                        <tbody>
                          {exportQueue.map((row) => (
                            <tr key={row.request_id || row.id}>
                              <td>{row.request_id}</td>
                              <td>{row.status}</td>
                              <td>{row.upserted_count ?? '—'}</td>
                              <td>{row.updated_at || row.created_at}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    ) : (
                      'No export requests in queue yet.'
                    )}
                  </div>
                </div>

                <div className="aic-divider" />
                <p className="aic-section-label">Manual export fetch</p>
                <p className="aic-field-hint">
                  After a recruiter exports a candidate in LinkedIn Recruiter, paste the requestId from
                  the push notification (valid 24 hours) to import immediately.
                </p>
                <div className="aic-queue">
                  <input
                    className="aic-manual-input"
                    value={manualRequestId}
                    onChange={(e) => setManualRequestId(e.target.value)}
                    placeholder="Export request ID"
                  />
                  <button
                    type="button"
                    className="aic-btn aic-btn-ghost"
                    disabled={manualFetching}
                    onClick={handleManualLinkedInFetch}
                  >
                    {manualFetching ? 'Fetching…' : 'Fetch candidate'}
                  </button>
                </div>
                <p className="aic-field-hint">
                  Requires LinkedIn Talent Solutions / RSC app access. Save credentials, run Test
                  connection, then register the webhook URL in LinkedIn.
                </p>
              </>
            )}
          </section>
        )}

        {activeTab === 'naukri' && (
          <>
            <section className="aic-card aic-connector-form" data-testid="admin-integrations-naukri">
              <div className="aic-section-title">
                <div>
                  <h2>Naukri Official API</h2>
                  <p>Client credentials and scopes used by the connector.</p>
                </div>
                <StatusPill {...enabledStatusLabel(!!configs.NAUKRI?.enabled)} />
              </div>
              <label className="aic-chip aic-chip-check">
                <input
                  type="checkbox"
                  checked={!!configs.NAUKRI?.enabled}
                  onChange={(e) => updateField('NAUKRI', 'enabled', e.target.checked)}
                />
                Enable Naukri connector
              </label>
              <div className="aic-form-row aic-form-row-spaced">
                <div className="aic-field">
                  <label htmlFor="naukri-client-id">Client ID</label>
                  <input
                    id="naukri-client-id"
                    value={configs.NAUKRI?.client_id || ''}
                    onChange={(e) => updateField('NAUKRI', 'client_id', e.target.value)}
                  />
                </div>
                <div className="aic-field">
                  <label htmlFor="naukri-client-secret">Client Secret / Token</label>
                  <input
                    id="naukri-client-secret"
                    type="password"
                    value={configs.NAUKRI?.client_secret || ''}
                    onChange={(e) => updateField('NAUKRI', 'client_secret', e.target.value)}
                    placeholder="Paste secret/token"
                  />
                </div>
              </div>
              <div className="aic-field">
                <label htmlFor="naukri-base-url">Base URL</label>
                <input
                  id="naukri-base-url"
                  value={configs.NAUKRI?.base_url || ''}
                  onChange={(e) => updateField('NAUKRI', 'base_url', e.target.value)}
                  placeholder="https://api.naukri.com/..."
                />
              </div>
              <div className="aic-field">
                <label htmlFor="naukri-scopes">Scopes (comma-separated)</label>
                <textarea
                  id="naukri-scopes"
                  value={configs.NAUKRI?.scopes || ''}
                  onChange={(e) => updateField('NAUKRI', 'scopes', e.target.value)}
                  placeholder="..."
                />
              </div>
              <div className="aic-divider" />
              <p className="aic-section-label">OAuth &amp; HTTP ingestion</p>
              <div className="aic-form-row">
                <div className="aic-field">
                  <label htmlFor="naukri-oauth-url">OAuth token URL (optional)</label>
                  <input
                    id="naukri-oauth-url"
                    value={configs.NAUKRI?.oauth_token_url || ''}
                    onChange={(e) => updateField('NAUKRI', 'oauth_token_url', e.target.value)}
                  />
                </div>
                <div className="aic-field">
                  <label htmlFor="naukri-refresh-token">Refresh token (optional)</label>
                  <input
                    id="naukri-refresh-token"
                    type="password"
                    value={configs.NAUKRI?.refresh_token || ''}
                    onChange={(e) => updateField('NAUKRI', 'refresh_token', e.target.value)}
                  />
                </div>
              </div>
              <div className="aic-form-row">
                <div className="aic-field">
                  <label htmlFor="naukri-page-size">Page size</label>
                  <input
                    id="naukri-page-size"
                    type="number"
                    min={1}
                    value={configs.NAUKRI?.page_size ?? ''}
                    onChange={(e) => updateField('NAUKRI', 'page_size', e.target.value)}
                  />
                </div>
                <div className="aic-field">
                  <label htmlFor="naukri-max-retries">Max retries</label>
                  <input
                    id="naukri-max-retries"
                    type="number"
                    min={1}
                    value={configs.NAUKRI?.max_retries ?? ''}
                    onChange={(e) => updateField('NAUKRI', 'max_retries', e.target.value)}
                  />
                </div>
              </div>
              <div className="aic-field">
                <label htmlFor="naukri-min-interval">Min interval (ms)</label>
                <input
                  id="naukri-min-interval"
                  type="number"
                  min={0}
                  value={configs.NAUKRI?.min_interval_ms ?? ''}
                  onChange={(e) => updateField('NAUKRI', 'min_interval_ms', e.target.value)}
                />
              </div>
            </section>

            <section className="aic-card aic-connector-form" data-testid="admin-integrations-naukri-mongo">
              <div className="aic-section-title">
                <div>
                  <h2>External Mongo fallback</h2>
                  <p>Used by Naukri / Monster when the API connector needs a secondary candidate store.</p>
                </div>
              </div>
              <div className="aic-form-row">
                <div className="aic-field">
                  <label htmlFor="naukri-mongo-url">Mongo URL</label>
                  <input
                    id="naukri-mongo-url"
                    value={configs.NAUKRI?.mongo_url || ''}
                    onChange={(e) => updateField('NAUKRI', 'mongo_url', e.target.value)}
                    placeholder="mongodb://host:27017"
                  />
                </div>
                <div className="aic-field">
                  <label htmlFor="naukri-db-name">DB Name</label>
                  <input
                    id="naukri-db-name"
                    value={configs.NAUKRI?.db_name || ''}
                    onChange={(e) => updateField('NAUKRI', 'db_name', e.target.value)}
                    placeholder="aai_hrms"
                  />
                </div>
              </div>
              <div className="aic-field">
                <label htmlFor="naukri-collection">Collection Name</label>
                <input
                  id="naukri-collection"
                  value={configs.NAUKRI?.collection_name || ''}
                  onChange={(e) => updateField('NAUKRI', 'collection_name', e.target.value)}
                  placeholder="candidates"
                />
              </div>
              <div className="aic-success">
                If provided, backend will try external Mongo ingestion first, then fall back to base_url.
              </div>
            </section>
          </>
        )}

        {activeTab === 'monster' && (
          <section className="aic-card aic-connector-form" data-testid="admin-integrations-monster">
            <div className="aic-section-title">
              <div>
                <h2>Monster Official API</h2>
                <p>Same ingestion stack as Naukri: OAuth, paging, retries, optional external Mongo.</p>
              </div>
              <StatusPill {...enabledStatusLabel(!!configs.MONSTER?.enabled)} />
            </div>
            <label className="aic-chip aic-chip-check">
              <input
                type="checkbox"
                checked={!!configs.MONSTER?.enabled}
                onChange={(e) => updateField('MONSTER', 'enabled', e.target.checked)}
              />
              Enable Monster connector
            </label>
            <div className="aic-form-row aic-form-row-spaced">
              <div className="aic-field">
                <label htmlFor="monster-client-id">Client ID</label>
                <input
                  id="monster-client-id"
                  value={configs.MONSTER?.client_id || ''}
                  onChange={(e) => updateField('MONSTER', 'client_id', e.target.value)}
                />
              </div>
              <div className="aic-field">
                <label htmlFor="monster-client-secret">Client Secret / Token</label>
                <input
                  id="monster-client-secret"
                  type="password"
                  value={configs.MONSTER?.client_secret || ''}
                  onChange={(e) => updateField('MONSTER', 'client_secret', e.target.value)}
                  placeholder="Paste secret/token"
                />
              </div>
            </div>
            <div className="aic-field">
              <label htmlFor="monster-base-url">Base URL</label>
              <input
                id="monster-base-url"
                value={configs.MONSTER?.base_url || ''}
                onChange={(e) => updateField('MONSTER', 'base_url', e.target.value)}
                placeholder="https://api.monster.com/..."
              />
            </div>
            <div className="aic-field">
              <label htmlFor="monster-scopes">Scopes (comma-separated)</label>
              <textarea
                id="monster-scopes"
                value={configs.MONSTER?.scopes || ''}
                onChange={(e) => updateField('MONSTER', 'scopes', e.target.value)}
                placeholder="..."
              />
            </div>
            <div className="aic-divider" />
            <p className="aic-section-label">OAuth &amp; HTTP ingestion</p>
            <div className="aic-form-row">
              <div className="aic-field">
                <label htmlFor="monster-oauth-url">OAuth token URL (optional)</label>
                <input
                  id="monster-oauth-url"
                  value={configs.MONSTER?.oauth_token_url || ''}
                  onChange={(e) => updateField('MONSTER', 'oauth_token_url', e.target.value)}
                />
              </div>
              <div className="aic-field">
                <label htmlFor="monster-refresh-token">Refresh token (optional)</label>
                <input
                  id="monster-refresh-token"
                  type="password"
                  value={configs.MONSTER?.refresh_token || ''}
                  onChange={(e) => updateField('MONSTER', 'refresh_token', e.target.value)}
                />
              </div>
            </div>
            <div className="aic-form-row">
              <div className="aic-field">
                <label htmlFor="monster-page-size">Page size</label>
                <input
                  id="monster-page-size"
                  type="number"
                  min={1}
                  value={configs.MONSTER?.page_size ?? ''}
                  onChange={(e) => updateField('MONSTER', 'page_size', e.target.value)}
                />
              </div>
              <div className="aic-field">
                <label htmlFor="monster-max-retries">Max retries</label>
                <input
                  id="monster-max-retries"
                  type="number"
                  min={1}
                  value={configs.MONSTER?.max_retries ?? ''}
                  onChange={(e) => updateField('MONSTER', 'max_retries', e.target.value)}
                />
              </div>
            </div>
            <div className="aic-field">
              <label htmlFor="monster-min-interval">Min interval (ms)</label>
              <input
                id="monster-min-interval"
                type="number"
                min={0}
                value={configs.MONSTER?.min_interval_ms ?? ''}
                onChange={(e) => updateField('MONSTER', 'min_interval_ms', e.target.value)}
              />
            </div>
            <div className="aic-divider" />
            <p className="aic-section-label">External Mongo (optional)</p>
            <div className="aic-form-row">
              <div className="aic-field">
                <label htmlFor="monster-mongo-url">Mongo URL</label>
                <input
                  id="monster-mongo-url"
                  value={configs.MONSTER?.mongo_url || ''}
                  onChange={(e) => updateField('MONSTER', 'mongo_url', e.target.value)}
                  placeholder="mongodb://host:27017"
                />
              </div>
              <div className="aic-field">
                <label htmlFor="monster-db-name">DB Name</label>
                <input
                  id="monster-db-name"
                  value={configs.MONSTER?.db_name || ''}
                  onChange={(e) => updateField('MONSTER', 'db_name', e.target.value)}
                  placeholder="aai_hrms"
                />
              </div>
            </div>
            <div className="aic-field">
              <label htmlFor="monster-collection">Collection Name</label>
              <input
                id="monster-collection"
                value={configs.MONSTER?.collection_name || ''}
                onChange={(e) => updateField('MONSTER', 'collection_name', e.target.value)}
                placeholder="candidates"
              />
            </div>
          </section>
        )}

        {activeTab === 'audit' && (
          <section className="aic-card aic-connector-form" data-testid="admin-integrations-audit">
            <div className="aic-section-title">
              <div>
                <h2>Connector audit trail</h2>
                <p>Recent configuration changes and integration events will appear here.</p>
              </div>
            </div>
            <div className="aic-queue-box aic-audit-placeholder">
              Audit trail placeholder — connector config change logging is planned for a future release.
            </div>
          </section>
        )}

        <div className="aic-sticky-save">{renderSaveButton('admin-integrations-save-footer')}</div>
      </div>
    </div>
  );
};

export default AdminIntegrationsPage;
