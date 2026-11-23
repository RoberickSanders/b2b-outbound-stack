from lead import detect_client, extract_geo, extract_target


def test_detect_client():
    assert detect_client("find 500 hotels in Tampa for client_a") == "client_a"
    assert detect_client("msps in texas for client_b") == "client_b"
    assert detect_client("cost seg firms") is None


def test_extract_target():
    assert extract_target("find 500 leads") == 500
    assert extract_target("pull 2k contacts") == 2000
    assert extract_target("cost seg firms") is None


def test_extract_geo():
    assert extract_geo("restaurants in florida") == "florida"
    assert extract_geo("hvac companies in Tampa") == "tampa"
