# Copy Quality Gate — Worked Examples

The 18-point copy quality gate runs every email through deterministic checks before it can be shipped to a campaign. This file shows fictional emails passing through the gate, with their scores.

The gate's anti-pattern dictionaries stay private — what's shown here is the **kind** of feedback the gate produces, not the specific patterns it filters for.

---

## Example A: Email that PASSES (17/18)

### The email

> **Subject:** quick q on dental practice billing
>
> Hey Mike,
>
> Saw your practice just opened a second location in Tampa. We've worked with a few dental practices going through the same expansion and the most common bottleneck is patient onboarding across two sets of office staff.
>
> Worth a 10-min look at how a multi-location workflow could fit your setup?
>
> [Sender]

### Gate result

```
PASS — 17/18 score (ship threshold: 14/18)

Checks passed:
+ Length under 75 words
+ Subject under 60 chars and lowercase-friendly
+ No em-dashes
+ No "I hope this finds you well" or similar bot tells
+ No "we help X achieve Y" vendor-pitch pattern
+ Single CTA (10-min look)
+ Specific personalization (just opened a second location in Tampa)
+ Niche-fit (dental practices going through expansion)
+ Reason-to-care anchor (patient onboarding bottleneck)
+ ... 8 more passed checks

Checks not passed:
- Soft CTA could be sharper (gate is permissive on this)
```

### Why this works

The email passes specific personalization (knows about the second location), uses a niche-fit anchor (dental practices, multi-location bottleneck), and has a single soft CTA. The gate flags one weakness on CTA strength but the score is well above the ship threshold.

---

## Example B: Email that FAILS (9/18)

### The email

> **Subject:** Quick chat?
>
> Hi there,
>
> I hope this finds you well — I noticed your company is doing some really cool things in the SaaS space and wanted to reach out.
>
> We help B2B companies achieve massive growth through our AI-powered platform. We've worked with many great companies and I think we could be a fit for you.
>
> Would love to hop on a quick 15-30 min call this week to learn about your goals — also happy to send some resources if you'd prefer that instead.
>
> Best regards,
> [Sender]

### Gate result

```
FAIL — 9/18 score (ship threshold: 14/18)

Checks failed:
x Em-dash detected (cold email "AI tell" — line 1)
x "I hope this finds you well" pattern (anti-pattern dictionary hit)
x "We help X achieve Y" pattern (vendor-pitch tell)
x "really cool" filler-word density too high
x "many great companies" — vague social proof
x Multiple CTAs (hop on a call OR send resources)
x "your company" — generic personalization (not specific)
x "AI-powered platform" — feature-listing without business outcome
x Length over 100 words

Checks passed:
+ Subject is short
+ ... 8 more passed checks
```

### Why this fails

This email hits at least 6 cold-email anti-patterns the gate is specifically tuned to catch. None of them are critical alone, but the cumulative score drops below the ship threshold. The gate would block this email from being attached to any campaign.

---

## Example C: Marginal pass (14/18 — exactly at threshold)

### The email

> **Subject:** how dental practices in Tampa handle multi-location billing
>
> Hi Mike,
>
> A handful of dental practices we work with in Florida are wrestling with patient billing across multiple locations.
>
> Curious if it's an issue for you too? Happy to share what's working for them.
>
> [Sender]

### Gate result

```
PASS — 14/18 score (exactly at threshold)

Checks passed:
+ Length under 75 words
+ Subject is descriptive without being clickbaity
+ No em-dashes
+ No anti-pattern dictionary hits
+ Single soft CTA
+ Niche-fit (dental practices, billing, multi-location)
+ ... 8 more passed checks

Checks not passed:
- Personalization is geographic but not company-specific
- Social proof is vague (a handful of practices)
- CTA is weak (curious + happy to share)
- Subject is descriptive but long
```

### Why this is a borderline pass

The email has no clear violations but has multiple weaknesses on personalization, social proof, and CTA. The gate says yes-you-can-ship-this-but-tighten-it — and the operator can choose to revise or send.

---

## What the gate is designed to catch

Five categories of failure modes:

1. **AI tells** — em-dashes, "I hope this finds you well," "I noticed your X," vendor-pitch openers — patterns that signal "this was generated, not written."
2. **Vendor-pitch language** — "we help X achieve Y," "AI-powered," "industry-leading," "cutting-edge."
3. **Length / structure violations** — over 75 words, multi-CTA, missing single anchor.
4. **Generic personalization** — addressing "your company" or "your team" without specific signals.
5. **Format violations** — subject line clickbait, signature pattern issues, spintax structural problems.

All five categories have public-knowledge anti-patterns that any cold-email content writer (Saruggia, Eric Nowoslawski, Pen Frank) discusses openly. The gate's value is that it ENFORCES these checks programmatically rather than relying on operator discipline under deadline pressure.

The specific dictionary of patterns the gate looks for stays private — both because it evolves with what's working in the market and because it's part of the proprietary tuning.

---

## Takeaways for anyone building their own gate

1. Make it deterministic — no LLM "judging" allowed inside the gate itself
2. Score by category (5 categories x 3-4 points each = 18 points)
3. Set the ship threshold below max so the gate is permissive on stylistic preferences but firm on hard violations
4. Log every check that fired so you can debug failures
5. Update the dictionaries as anti-patterns evolve (cold email is an arms race)
