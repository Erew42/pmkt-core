from pmkt.market_structure.grouping import Interval, canonical_signature, parse_interval

def test_parse_interval_between():
    res = parse_interval("Will ETH be between $3,000 and $4k on Jan 1?")
    assert res == Interval(low=3000.0, high=4000.0, low_inclusive=True, high_inclusive=True, kind="range")

def test_parse_interval_from_to():
    res = parse_interval("From 1m to 2.5m users")
    assert res == Interval(low=1000000.0, high=2500000.0, low_inclusive=True, high_inclusive=True, kind="range")

def test_parse_interval_hyphen():
    res = parse_interval("1.5 - 2.5")
    assert res == Interval(low=1.5, high=2.5, low_inclusive=True, high_inclusive=True, kind="range")

def test_parse_interval_lte():
    res = parse_interval("at most 50")
    assert res == Interval(low=None, high=50.0, low_inclusive=False, high_inclusive=True, kind="lte")

    res2 = parse_interval("<= 10.5")
    assert res2 == Interval(low=None, high=10.5, low_inclusive=False, high_inclusive=True, kind="lte")

def test_parse_interval_gte_trailing():
    res = parse_interval("100 or more")
    assert res == Interval(low=100.0, high=None, low_inclusive=True, high_inclusive=False, kind="gte")

    res2 = parse_interval("1.5M+")
    assert res2 == Interval(low=1500000.0, high=None, low_inclusive=True, high_inclusive=False, kind="gte")

def test_parse_interval_lt():
    res = parse_interval("under 100k")
    assert res == Interval(low=None, high=100000.0, low_inclusive=False, high_inclusive=False, kind="lt")

def test_parse_interval_gt():
    res = parse_interval("over 5.5")
    assert res == Interval(low=5.5, high=None, low_inclusive=False, high_inclusive=False, kind="gt")

def test_canonical_signature_basic():
    sig = canonical_signature("Will ETH be between $3000 and $4000?")
    assert "range NUM" in sig
    assert "eth" in sig

def test_canonical_signature_trailing():
    sig = canonical_signature("User count 1.5M+")
    assert "user count" in sig
    assert "NUM" in sig
