from copy_quality_gate import grade_sequence, humanizer_scan, rubric_checks
from fixtures import good_sequence


def _set_body(seq, step, label, body):
    for s in seq["sequences"]:
        if s["seq_number"] == step:
            for v in s["seq_variants"]:
                if v["variant_label"] == label:
                    v["email_body"] = body
                    return seq
    raise KeyError((step, label))


def test_good_sequence_ships():
    result = grade_sequence(good_sequence())
    assert result["ship_ok"] is True
    assert result["score"] >= result["min_required"]
    assert result["total"] == 18
    assert result["humanizer_flags"] == []


def test_rubric_has_18_named_rules():
    names = [c[0] for c in rubric_checks(good_sequence())]
    assert len(names) == 18
    assert len(set(names)) == 18


def test_em_dash_blocks_ship_even_with_high_score():
    seq = _set_body(good_sequence(), 2, "A",
                    "<div>{{first_name}}, want me to send the checklist \u2014 free, no pitch?</div>")
    result = grade_sequence(seq)
    assert any(h["pattern"] == "em_dash" for h in result["humanizer_flags"])
    assert result["ship_ok"] is False


def test_following_up_phrase_is_flagged():
    seq = _set_body(good_sequence(), 2, "A",
                    "<div>{{first_name}}, just following up. Want me to send the checklist?</div>")
    patterns = {h["pattern"] for h in humanizer_scan(seq)}
    assert "following_up" in patterns


def test_fabricated_frequency_claim_is_flagged():
    seq = _set_body(good_sequence(), 1, "A",
                    "<div>Hi {{first_name}}, most buildings {fail|miss} NFPA 25 inspections. "
                    "Want a free walkthrough?</div>")
    patterns = {h["pattern"] for h in humanizer_scan(seq)}
    assert "frequency_most" in patterns


def test_over_75_words_fails_rule_1():
    long_body = "<div>Hi {{first_name}}, " + "your NFPA inspection " * 40 + "call?</div>"
    seq = _set_body(good_sequence(), 1, "A", long_body)
    failed = {f["rule"] for f in grade_sequence(seq)["failed"]}
    assert "1_word_count_75" in failed


def test_min_score_is_respected():
    assert grade_sequence(good_sequence(), min_score=18)["ship_ok"] is False
