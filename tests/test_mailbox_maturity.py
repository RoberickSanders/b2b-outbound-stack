from datetime import datetime, timedelta, timezone

from mailbox_helpers import MIN_AGE_DAYS, MIN_WARMUP_PCT, filter_by_client, is_mature


def _acct(age_days, warmup="100%", email="sender@client_a-mail.example.com"):
    created = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    return {"id": 1, "from_email": email, "created_at": created,
            "warmup_details": {"warmup_reputation": warmup}}


def test_defaults_are_the_documented_rule():
    assert MIN_AGE_DAYS == 14
    assert MIN_WARMUP_PCT == 100


def test_old_and_fully_warm_is_mature():
    assert is_mature(_acct(30)) is True


def test_young_mailbox_is_not_mature_even_at_100pct():
    # Warmup reputation can hit 100% in 2-3 days; age still blocks it.
    assert is_mature(_acct(3)) is False


def test_old_but_not_fully_warm_is_not_mature():
    assert is_mature(_acct(30, warmup="97%")) is False


def test_unparseable_fields_fail_closed():
    assert is_mature({"created_at": "garbage", "warmup_details": {"warmup_reputation": "n/a"}}) is False


def test_filter_by_client_uses_neutral_keys():
    accts = [_acct(30, email="a@client_a-mail.example.com"), _acct(30, email="b@client_b-mail.example.com")]
    assert [a["from_email"] for a in filter_by_client(accts, "client_a")] == ["a@client_a-mail.example.com"]
