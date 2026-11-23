import pytest

from verify_title_redflags import is_bad_title


@pytest.mark.parametrize("title,bad", [
    ("Marketing Intern", True),
    ("Executive Assistant to the CEO", True),
    ("Receptionist", True),
    ("Retired", True),
    ("Assistant General Manager", False),
    ("Assistant Vice President, Facilities", False),
    ("Owner", False),
    ("Director of Operations", False),
    ("", False),
    (None, False),
])
def test_is_bad_title(title, bad):
    assert is_bad_title(title) is bad
