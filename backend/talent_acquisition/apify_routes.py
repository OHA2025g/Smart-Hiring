"""Admin + job routes for Apify LinkedIn pipeline."""

from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable, Dict

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from talent_acquisition.apify_linkedin_connector import (
    APIFY_RUNS_COLLECTION,
    CONNECTOR_COLL,
    CONNECTOR_NAME,
    PIPELINE_ACTIVE_STATUSES,
    ensure_apify_linkedin_defaults,
    get_apify_token,
    process_pending_apify_pipelines,
    start_apify_pipeline_for_job,
    test_apify_connection,
    validate_apify_config,
    _public_pipeline,
)

logger = logging.getLogger(__name__)


class ApifyTestResponse(BaseModel):
    ok: bool
    message: str
    run_id: str | None = None
    search_actor_id: str | None = None
    enrich_actor_id: str | None = None


def _cron_token_ok(request_token: str | None) -> bool:
    expected = (
        os.environ.get("APIFY_LINKEDIN_PROCESS_TOKEN")
        or os.environ.get("LINKEDIN_EXPORT_PROCESS_TOKEN")
        or os.environ.get("HIRING_SNAPSHOT_TOKEN")
        or "docker_dev_hiring_snapshot_token"
    )
    return bool(request_token) and request_token == expected


def create_apify_linkedin_router(
    *,
    db,
    get_current_user: Callable,
    require_admin: Callable,
    upsert_candidate: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
) -> APIRouter:
    router = APIRouter(tags=["apify-linkedin"])

    async def _load_cfg() -> Dict[str, Any]:
        return await db[CONNECTOR_COLL].find_one({"name": CONNECTOR_NAME}, {"_id": 0}) or {}

    @router.get("/admin/apify-linkedin/status")
    async def apify_status(current_user: dict = Depends(get_current_user)):
        require_admin(current_user)
        await ensure_apify_linkedin_defaults(db)
        cfg = await _load_cfg()
        ok, msg = validate_apify_config(cfg)
        return {
            "enabled": bool(cfg.get("enabled")),
            "api_mode": cfg.get("api_mode"),
            "configured": ok,
            "configuration_message": msg,
            "token_set": bool(get_apify_token()),
            "apify_search_actor_id": cfg.get("apify_search_actor_id"),
            "apify_enrich_actor_id": cfg.get("apify_enrich_actor_id"),
            "apify_email_actor_id": cfg.get("apify_email_actor_id"),
            "apify_email_fallback_enabled": bool(cfg.get("apify_email_fallback_enabled", True)),
            "apify_max_results_per_search": cfg.get("apify_max_results_per_search"),
            "apify_default_geocode": cfg.get("apify_default_geocode"),
        }

    @router.post("/admin/apify-linkedin/test", response_model=ApifyTestResponse)
    async def apify_test(current_user: dict = Depends(get_current_user)):
        require_admin(current_user)
        await ensure_apify_linkedin_defaults(db)
        cfg = await _load_cfg()
        result = await test_apify_connection(cfg)
        return ApifyTestResponse(**result)

    @router.post("/admin/apify-linkedin/process-cron")
    async def apify_process_cron(request: Request):
        token = request.headers.get("X-Apify-LinkedIn-Process-Token")
        if not _cron_token_ok(token):
            raise HTTPException(status_code=401, detail="Invalid cron token")
        await ensure_apify_linkedin_defaults(db)
        cfg = await _load_cfg()
        return await process_pending_apify_pipelines(db, cfg, upsert_candidate)

    @router.post("/admin/apify-linkedin/process-run/{pipeline_id}")
    async def apify_process_run(
        pipeline_id: str,
        current_user: dict = Depends(get_current_user),
    ):
        require_admin(current_user)
        cfg = await _load_cfg()
        doc = await db[APIFY_RUNS_COLLECTION].find_one({"id": pipeline_id}, {"_id": 0})
        if not doc:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        if doc.get("status") not in PIPELINE_ACTIVE_STATUSES:
            return {"message": "Pipeline not active", "pipeline": doc}
        await process_pending_apify_pipelines(
            db, cfg, upsert_candidate, limit=1, pipeline_id=pipeline_id
        )
        updated = await db[APIFY_RUNS_COLLECTION].find_one({"id": pipeline_id}, {"_id": 0})
        pipeline = _public_pipeline(updated) if updated else None
        return {"message": "Pipeline advanced", "pipeline": pipeline}

    @router.get("/jobs/{job_id}/apify-linkedin/run")
    async def apify_job_run_status(
        job_id: str,
        current_user: dict = Depends(get_current_user),
    ):
        """Return latest pipeline status.

        Also advances an active pipeline one step. EasyPanel / single-container
        deploys often lack the apify-linkedin-process-cron service; the job UI
        already polls this endpoint every ~12s, so advancing here unblocks
        search_running → completed without a separate cron.
        """
        _ = current_user
        await ensure_apify_linkedin_defaults(db)
        doc = await db[APIFY_RUNS_COLLECTION].find_one(
            {"job_id": job_id},
            {"_id": 0},
            sort=[("created_at", -1)],
        )
        if doc and doc.get("status") in PIPELINE_ACTIVE_STATUSES:
            cfg = await _load_cfg()
            try:
                await process_pending_apify_pipelines(
                    db,
                    cfg,
                    upsert_candidate,
                    limit=1,
                    pipeline_id=str(doc.get("id") or ""),
                )
            except Exception:
                logger.exception("Apify pipeline advance on status poll failed for job %s", job_id)
            doc = await db[APIFY_RUNS_COLLECTION].find_one(
                {"id": doc.get("id")},
                {"_id": 0},
            ) or doc
        return {
            "job_id": job_id,
            "pipeline": _public_pipeline(doc) if doc else None,
        }

    @router.post("/jobs/{job_id}/apify-linkedin/search")
    async def apify_start_job_search(
        job_id: str,
        current_user: dict = Depends(get_current_user),
    ):
        _ = current_user
        job = await db.jobs.find_one({"id": job_id}, {"_id": 0})
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        cfg = await _load_cfg()
        return await start_apify_pipeline_for_job(db, job, cfg, upsert_candidate=upsert_candidate)

    return router
