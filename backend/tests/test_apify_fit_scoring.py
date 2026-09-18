"""Fit scoring for Apify/LinkedIn sourced candidates."""

from server import compute_basic_fit_score


def test_linkedin_apify_candidate_can_score_above_80():
    job = {
        "title": "Senior Python Engineer",
        "normalized_title": "Senior Python Engineer",
        "description": "Build data pipelines with Python and Spark on AWS.",
        "skills": [
            {"skill_name": "Python", "skill_type": "MUST_HAVE"},
            {"skill_name": "Spark", "skill_type": "NICE_TO_HAVE"},
            {"skill_name": "AWS", "skill_type": "NICE_TO_HAVE"},
        ],
        "min_experience_years": 4,
        "scoring_rubric": {"weights": {"title": 0.2, "skill": 0.4, "activity": 0.3, "experience": 0.1}},
    }
    candidate = {
        "full_name": "Jane Doe",
        "headline": "Senior Python Engineer | Spark | AWS",
        "source": "LINKEDIN",
        "import_metadata": {"provider": "apify"},
        "skills": [
            {"skill_name": "Python (Programming Language)"},
            {"skill_name": "Apache Spark"},
            {"skill_name": "Amazon Web Services (AWS)"},
        ],
        "resume_text": (
            "Senior Python Engineer building Spark pipelines on AWS. "
            "Python Spark AWS data engineering."
        ),
        "total_experience_years": 6,
    }
    score = compute_basic_fit_score(job, candidate)
    assert score["skill_match_pct"] >= 66
    assert score["final_score"] >= 80
    assert score["must_have_ok"] is True


def test_linkedin_soft_matches_skills_from_resume_text():
    job = {
        "title": "Data Engineer",
        "skills": [
            {"skill_name": "Python", "skill_type": "MUST_HAVE"},
            {"skill_name": "SQL", "skill_type": "MUST_HAVE"},
        ],
        "scoring_rubric": {"weights": {"title": 0.2, "skill": 0.4, "activity": 0.3, "experience": 0.1}},
    }
    candidate = {
        "headline": "Data Engineer",
        "source": "LINKEDIN",
        "import_metadata": {"provider": "apify"},
        "skills": [],
        "resume_text": "Data Engineer with strong Python and SQL background.",
        "total_experience_years": 5,
    }
    score = compute_basic_fit_score(job, candidate)
    assert score["skill_match_pct"] == 100
    assert score["final_score"] >= 70


def test_wealth_manager_linkedin_scores_above_70():
    """Duty-phrase must-haves + marketing headlines used to crush overall fit (~45%)."""
    job = {
        "title": "Wealth Manager",
        "skills": [
            {"skill_name": "Monitor and address HNI clients", "skill_type": "MUST_HAVE"},
            {"skill_name": "Investment Banking", "skill_type": "MUST_HAVE"},
            {"skill_name": "Business Development", "skill_type": "MUST_HAVE"},
            {"skill_name": "Inbound Sales", "skill_type": "MUST_HAVE"},
            {"skill_name": "Outbond Sales", "skill_type": "MUST_HAVE"},
            {"skill_name": "Banking and Investment", "skill_type": "GOOD_TO_HAVE"},
            {"skill_name": "Cross Selling", "skill_type": "GOOD_TO_HAVE"},
        ],
        "scoring_rubric": {"weights": {"title": 0.2, "skill": 0.4, "activity": 0.3, "experience": 0.1}},
    }
    candidate = {
        "full_name": "Abhishek Roychowdhury",
        "headline": (
            "Helping ambitious dreamers become Ultra High-Net-Worth Individuals. "
            "Ex SBI MF | Ex IDFC MF | AMFI Registered Mutual Fund Distributor"
        ),
        "source": "LINKEDIN",
        "import_metadata": {"provider": "apify"},
        "skills": [
            {"skill_name": "Investment Banking"},
            {"skill_name": "Business Development"},
            {"skill_name": "Relationship Management"},
            {"skill_name": "Portfolio Management"},
            {"skill_name": "Banking"},
            {"skill_name": "Finance"},
        ],
        "resume_text": (
            "Investment Banking Business Development Relationship Management "
            "Portfolio Management Banking inbound outbound sales cross selling"
        ),
        "total_experience_years": None,
    }
    score = compute_basic_fit_score(job, candidate)
    assert score["title_score"] >= 70
    assert score["skill_match_pct"] >= 70
    assert score["must_have_ok"] is True
    assert "monitor and address hni clients" not in score["explanation"]["missing_must_have"]
    assert score["final_score"] >= 70
