"""A fictional 3-step sequence (Example Co, synthetic copy) that passes the ship gate."""
import copy

BODY_A = ("<div>Hi {{first_name}},</div><div>{Quick|Short} question: when is your next NFPA 25 "
          "inspection due? Missed dates show up as citations at renewal time.</div>"
          "<div>Want a free 15 minute walkthrough so you know what you'd be flagged for?</div>")
BODY_B = ("<div>Hi {{first_name}}, I'll cut the fluff and get right to it.</div><div>Your NFPA 72 "
          "alarm test is a {common|frequent} audit item. We check it for free.</div>"
          "<div>Worth a quick call?</div>")
BODY_C = ("<div>Hi {{first_name}},</div><div>If the state fire marshal walked in this week, "
          "{what|which items} would your compliance file be missing?</div>"
          "<div>Happy to send a sample checklist. Reply yes?</div>")
BODY_2 = ("<div>{{first_name}}, want me to send you the inspection checklist we use? "
          "It is free, no pitch. Reply and I'll send it.</div>")
BODY_3B = ("<div>{{first_name}}, I'll stop here. If {{company_name}} ever needs a second look "
           "at your inspection dates, just reply.</div>")
BODY_3A = "<div>{{first_name}}, last note from me. Should I close your file? Reply either way.</div>"

GOOD_SEQUENCE = {
    "sequences": [
        {"seq_number": 1, "seq_variants": [
            {"variant_label": "A", "subject": "inspection date question", "email_body": BODY_A},
            {"variant_label": "B", "subject": "fire alarm test", "email_body": BODY_B},
            {"variant_label": "C", "subject": "marshal visit this week", "email_body": BODY_C},
        ]},
        {"seq_number": 2, "seq_variants": [
            {"variant_label": "A", "subject": "", "email_body": BODY_2},
        ]},
        {"seq_number": 3, "seq_variants": [
            {"variant_label": "A", "subject": "", "email_body": BODY_3A},
            {"variant_label": "B", "subject": "", "email_body": BODY_3B},
        ]},
    ]
}


def good_sequence():
    return copy.deepcopy(GOOD_SEQUENCE)
