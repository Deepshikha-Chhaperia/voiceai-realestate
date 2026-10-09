"""Conservative explicit self-identification only. Repeat is an independent modifier."""
import re


def classify(text):
    t=str(text or '').lower().strip()
    patterns={
      'wrong_number':r'\b(?:wrong number|galat number)\b|गलत नंबर',
      'job':r'\b(?:looking for (?:a )?job|job vacancy|hiring|naukri chahiye)\b|नौकरी चाहिए',
      'broker':r'\b(?:i am (?:a )?(?:broker|agent)|main broker|mai broker)\b|मैं ब्रोकर',
      'buyer':r'\b(?:looking to buy|want to buy|buy a (?:flat|house)|property|bhk|budget|site visit|brochure)\b|फ्लैट|बजट|खरीद'}
    matches=[k for k,p in patterns.items() if re.search(p,t)]
    # Mixed/quoted/question forms stay unresolved. Never treat a lone yes/no/numeric choice as junk.
    if len(matches)!=1 or re.search(r"\b(?:not a broker|not looking for a job|is this|are you|my (?:friend|brother)|don't|dont)\b",t):return 'unknown'
    return matches[0]


def human_requested(text):
    t=str(text or '').lower()
    return bool(re.search(r'\b(?:speak|talk|connect|transfer).{0,24}\b(?:human|person|manager|advisor|representative)|\bhuman please\b|इंसान से|मैनेजर से बात',t)
      and not re.search(r"\b(?:don't|dont|not|no need)\b",t))
