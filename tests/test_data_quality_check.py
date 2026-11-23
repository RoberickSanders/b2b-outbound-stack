import csv

import pytest

from data_quality_check import audit_csv, evaluate, is_generic_email


@pytest.mark.parametrize("email,expected", [
    ("info@example.com", True),
    ("customer-service@example.com", True),
    ("Sales@Example.com", True),
    ("jane.doe@example.com", False),
    ("not-an-email", False),
    ("", False),
])
def test_is_generic_email(email, expected):
    assert is_generic_email(email) is expected


def _write_csv(path, rows):
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["email", "first_name", "last_name", "company_name", "website", "mv_result"])
        w.writeheader()
        w.writerows(rows)


def test_audit_csv_counts(tmp_path):
    p = tmp_path / "leads.csv"
    _write_csv(p, [
        {"email": "jane@example.com", "first_name": "Jane", "last_name": "Doe", "company_name": "Example Co", "website": "example.com", "mv_result": "ok"},
        {"email": "info@example.org", "first_name": "", "last_name": "", "company_name": "Example Org", "website": "example.org", "mv_result": "catch_all"},
        {"email": "jane@example.com", "first_name": "Jane", "last_name": "Doe", "company_name": "Example Co", "website": "example.com", "mv_result": "ok"},
        {"email": "bad-address", "first_name": "Sam", "last_name": "", "company_name": "", "website": "", "mv_result": ""},
    ])
    m = audit_csv(p)
    assert m["total"] == 4
    assert m["first_name_present"] == 3
    assert m["generic_emails"] == 1
    assert m["duplicate_count"] == 1
    assert m["invalid_email_format"] == 1
    assert m["mv_ok"] == 2
    assert m["mv_other_breakdown"] == {"catch_all": 1, "(missing)": 1}


def test_audit_csv_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        audit_csv(tmp_path / "nope.csv")


def _metrics(**kw):
    base = {"first_name_pct": 90, "company_name_pct": 99, "mv_ok_pct": 95,
            "generic_email_pct": 5, "duplicate_email_pct": 0}
    base.update(kw)
    return base


def test_evaluate_pass_warn_fail():
    assert evaluate(_metrics(), "b2b")["verdict"] == "PASS"
    assert evaluate(_metrics(first_name_pct=70), "b2b")["verdict"] == "WARN"
    assert evaluate(_metrics(first_name_pct=10), "b2b")["verdict"] == "FAIL"


def test_evaluate_vertical_thresholds_differ():
    m = _metrics(first_name_pct=45, generic_email_pct=45)
    assert evaluate(m, "b2b")["verdict"] == "FAIL"
    assert evaluate(m, "local")["verdict"] == "PASS"


def test_strict_mode_upgrades_warn():
    assert evaluate(_metrics(first_name_pct=70), "b2b", strict=True)["verdict"] == "FAIL"
