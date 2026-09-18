"""Order job AI matches: excel | talent pool | AI >90% per grid row (3 columns)."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

from talent_acquisition.candidate_source import (
    is_ai_generated_candidate,
    is_excel_imported_candidate,
    is_inhouse_database_candidate,
    is_real_linkedin_candidate,
    is_talent_pool_only_candidate,
)

DEFAULT_TOTAL_MATCH_LIMIT = 50
AI_HIGH_MATCH_MIN_SCORE = 90.0  # inclusive: seeded fits use 90% as top tier
LINKEDIN_FIRST_HIGH_FIT_MIN = 80.0


def _final_score(match: Dict[str, Any]) -> float:
    fit = match.get("fit_score") or {}
    try:
        return float(fit.get("final_score") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _is_ai_grid_match(
    cand: Dict[str, Any],
    score: float,
    *,
    job_id: str | None = None,
    ai_min_score: float = AI_HIGH_MATCH_MIN_SCORE,
) -> bool:
    """AI column: seeded per-job fits (70–90%) or any AI profile at/above 90%."""
    if not is_ai_generated_candidate(cand):
        return False
    if score >= ai_min_score:
        return True
    if job_id and cand.get("seed_job_id") and str(cand.get("seed_job_id")) == str(job_id):
        return True
    return False


def _partition_matches(
    results: List[Dict[str, Any]],
    *,
    job_id: str | None = None,
    ai_min_score: float = AI_HIGH_MATCH_MIN_SCORE,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    excel: List[Dict[str, Any]] = []
    talent_pool: List[Dict[str, Any]] = []
    ai_high: List[Dict[str, Any]] = []
    other: List[Dict[str, Any]] = []

    for row in results:
        cand = row.get("candidate") or {}
        score = _final_score(row)
        if is_excel_imported_candidate(cand):
            excel.append(row)
        elif _is_ai_grid_match(cand, score, job_id=job_id, ai_min_score=ai_min_score):
            ai_high.append(row)
        elif is_talent_pool_only_candidate(cand):
            talent_pool.append(row)
        else:
            other.append(row)

    excel.sort(key=_final_score, reverse=True)
    talent_pool.sort(key=_final_score, reverse=True)
    ai_high.sort(key=_final_score, reverse=True)
    other.sort(key=_final_score, reverse=True)
    return excel, talent_pool, ai_high, other


def order_job_match_results(
    results: List[Dict[str, Any]],
    *,
    job_id: str | None = None,
    total_limit: int = DEFAULT_TOTAL_MATCH_LIMIT,
    ai_min_score: float = AI_HIGH_MATCH_MIN_SCORE,
) -> List[Dict[str, Any]]:
    """
    Grid order (3 per row): Excel import | Talent pool (non-Excel) | AI-generated with fit 90%+.
    Repeats for up to `total_limit` matches; fills gaps from remaining buckets when a slot is empty.
    """
    total_limit = max(1, int(total_limit))
    excel, talent_pool, ai_high, other = _partition_matches(
        results, job_id=job_id, ai_min_score=ai_min_score
    )

    pools: List[List[Dict[str, Any]]] = [excel, talent_pool, ai_high]
    indices = [0, 0, 0]
    ordered: List[Dict[str, Any]] = []
    pos = 0

    while len(ordered) < total_limit:
        slot = pos % 3
        placed = False
        for offset in range(3):
            bucket = (slot + offset) % 3
            pool = pools[bucket]
            idx = indices[bucket]
            if idx < len(pool):
                ordered.append(pool[idx])
                indices[bucket] = idx + 1
                placed = True
                break
        if not placed:
            if other:
                ordered.append(other.pop(0))
                placed = True
            else:
                break
        pos += 1

    return ordered


def order_job_match_results_linkedin_first(
    results: List[Dict[str, Any]],
    *,
    total_limit: int = DEFAULT_TOTAL_MATCH_LIMIT,
    high_fit_min: float = LINKEDIN_FIRST_HIGH_FIT_MIN,
) -> List[Dict[str, Any]]:
    """LinkedIn search flow with high-fit priority.

    Order:
      1) LinkedIn ≥80%
      2) Inhouse/other ≥80%
      3) Remaining LinkedIn
      4) Remaining inhouse/other

    This keeps LinkedIn high-fits first while preventing thin Apify rows from
    burying strong talent-pool matches under the top-N limit.
    """
    total_limit = max(1, int(total_limit))
    linkedin: List[Dict[str, Any]] = []
    inhouse: List[Dict[str, Any]] = []
    other: List[Dict[str, Any]] = []

    for row in results:
        cand = row.get("candidate") or {}
        if is_real_linkedin_candidate(cand):
            linkedin.append(row)
        elif is_inhouse_database_candidate(cand):
            inhouse.append(row)
        else:
            other.append(row)

    linkedin.sort(key=_final_score, reverse=True)
    inhouse.sort(key=_final_score, reverse=True)
    other.sort(key=_final_score, reverse=True)

    li_high = [r for r in linkedin if _final_score(r) >= high_fit_min]
    li_rest = [r for r in linkedin if _final_score(r) < high_fit_min]
    in_high = [r for r in inhouse if _final_score(r) >= high_fit_min]
    in_rest = [r for r in inhouse if _final_score(r) < high_fit_min]
    ot_high = [r for r in other if _final_score(r) >= high_fit_min]
    ot_rest = [r for r in other if _final_score(r) < high_fit_min]

    ordered = li_high + in_high + ot_high + li_rest + in_rest + ot_rest
    return ordered[:total_limit]


def count_match_buckets(
    results: List[Dict[str, Any]],
    *,
    job_id: str | None = None,
    ai_min_score: float = AI_HIGH_MATCH_MIN_SCORE,
) -> Dict[str, int]:
    excel, talent_pool, ai_high, other = _partition_matches(
        results, job_id=job_id, ai_min_score=ai_min_score
    )
    return {
        "excel_count": len(excel),
        "talent_pool_count": len(talent_pool),
        "ai_high_match_count": len(ai_high),
        "other_count": len(other),
    }
