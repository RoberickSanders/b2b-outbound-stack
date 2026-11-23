import pytest

import utils


def test_slugify():
    assert utils.slugify("Fire Protection, Tampa!") == "fire-protection-tampa"
    assert utils.slugify("") == ""


@pytest.mark.parametrize("url,expected", [
    ("https://www.example.com/about", "example.com"),
    ("example.com", "example.com"),
    ("http://sub.example.co.uk:8080/x", "sub.example.co.uk"),
    ("", ""),
])
def test_get_domain(url, expected):
    assert utils.get_domain(url) == expected


def test_classify_email():
    assert utils.classify_email("info@example.com") == "generic"
    assert utils.classify_email("jane.doe@example.com") == "personal"


def test_is_junk_email():
    assert utils.is_junk_email("logo@2x.png") is True
    assert utils.is_junk_email("user@sentry.io") is True
    assert utils.is_junk_email("jane@acme-roofing.com") is False
    assert utils.is_junk_email("") is True


@pytest.mark.parametrize("name,expected", [
    ("Jane Doe", True),
    ("Mary-Kate O'Neil", True),
    ("Mary", True),
    ("Qwerty", False),
    ("Example Services LLC", False),
    ("Office Team", False),
    ("J4ne Doe", False),
    ("", False),
])
def test_is_valid_person_name(name, expected):
    assert utils.is_valid_person_name(name) is expected


def test_retry_with_backoff_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(utils.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    assert utils.retry_with_backoff(flaky, retries=2) == "ok"
    assert calls["n"] == 3


def test_retry_with_backoff_reraises(monkeypatch):
    monkeypatch.setattr(utils.time, "sleep", lambda s: None)
    with pytest.raises(RuntimeError):
        utils.retry_with_backoff(lambda: (_ for _ in ()).throw(RuntimeError("down")), retries=1)


def test_geocode_city_offline():
    assert utils.geocode_city("Tampa") == pytest.approx((27.9506, -82.4572))
    assert utils.geocode_city("Nowhereville") is None
