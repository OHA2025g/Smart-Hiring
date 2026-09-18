"""LinkedIn-first match ordering for Search LinkedIn flow."""

from talent_acquisition.match_ordering import (
    order_job_match_results,
    order_job_match_results_linkedin_first,
)


def _row(candidate, score):
    return {"candidate": candidate, "fit_score": {"final_score": score}}


def test_linkedin_first_puts_high_fit_linkedin_before_inhouse():
    linkedin = _row(
        {
            "id": "li-1",
            "source": "LINKEDIN",
            "linkedin_url": "https://www.linkedin.com/in/jane",
            "import_metadata": {"provider": "apify"},
        },
        85,
    )
    inhouse = _row({"id": "tp-1", "source": "BULK_SEED", "full_name": "Bulk Seed 1"}, 95)
    ordered = order_job_match_results_linkedin_first([inhouse, linkedin])
    assert ordered[0]["candidate"]["id"] == "li-1"
    assert ordered[1]["candidate"]["id"] == "tp-1"


def test_linkedin_first_surfaces_high_fit_inhouse_before_low_fit_linkedin():
    linkedin_low = _row(
        {
            "id": "li-low",
            "source": "LINKEDIN",
            "linkedin_url": "https://www.linkedin.com/in/low",
            "import_metadata": {"provider": "apify"},
        },
        55,
    )
    inhouse_high = _row({"id": "tp-high", "source": "BULK_SEED", "full_name": "Bulk Seed 1"}, 92)
    ordered = order_job_match_results_linkedin_first([linkedin_low, inhouse_high])
    assert ordered[0]["candidate"]["id"] == "tp-high"
    assert ordered[1]["candidate"]["id"] == "li-low"


def test_grid_order_unchanged_by_default():
    excel = _row(
        {"id": "ex-1", "seed_marker": "excel_candidates_v1", "source": "EXCEL_IMPORT"},
        80,
    )
    talent = _row({"id": "tp-1", "source": "BULK_SEED"}, 75)
    ai = _row(
        {
            "id": "ai-1",
            "seed_marker": "job_posting_fit_candidates_v1",
            "email": "fitseed.x@aai-hrms.local",
        },
        92,
    )
    ordered = order_job_match_results([talent, ai, excel], job_id="job-1")
    assert ordered[0]["candidate"]["id"] == "ex-1"
    assert ordered[1]["candidate"]["id"] == "tp-1"
    assert ordered[2]["candidate"]["id"] == "ai-1"
