//! The merge digest is of Python's compact sorted ASCII JSON, not raw frame bytes.
//! For a fractional JSON number the historical digest uses the shortest decimal for
//! its nearest binary64. Compute that spelling with integers so it can never become
//! a price or round a level in the lossless wire value.
use crate::Venue;
use blake2::{Blake2b, Digest, digest::consts::U16};
use num_bigint::BigUint;
use serde_json::Value;
use std::{cmp::Ordering, fmt::Write};

pub(super) type ContentId = [u8; 16];

pub(super) fn digest(venue: Venue, v: &Value) -> ContentId {
    let mut json = String::new();
    encode(v, &mut json, venue == Venue::Kalshi);
    Blake2b::<U16>::digest(json.as_bytes()).into()
}

fn string(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ' '..='~' => out.push(c),
            _ => {
                for unit in c.encode_utf16(&mut [0; 2]) {
                    write!(out, "\\u{unit:04x}").unwrap();
                }
            }
        }
    }
    out.push('"');
}
fn encode(v: &Value, out: &mut String, envelope: bool) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::String(s) => string(s, out),
        Value::Number(n) => {
            let raw = n.as_str();
            if raw.contains(['.', 'e', 'E']) {
                out.push_str(&float_spelling(raw));
            } else {
                out.push_str(if raw == "-0" { "0" } else { raw });
            }
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                encode(item, out, false);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            let mut first = true;
            // Sort explicitly: a consumer may enable serde_json's preserve_order feature.
            let mut keys: Vec<_> = map
                .keys()
                .filter(|k| !envelope || !matches!(k.as_str(), "sid" | "seq" | "sending_ts_ms"))
                .collect();
            keys.sort_unstable();
            for k in keys {
                if !first {
                    out.push(',');
                }
                first = false;
                string(k, out);
                out.push(':');
                encode(&map[k], out, false);
            }
            out.push('}');
        }
    }
}

fn pow10(n: i64) -> BigUint {
    BigUint::from(10u8).pow(n as u32)
}
fn divide_nearest(n: &BigUint, d: &BigUint) -> BigUint {
    let q = n / d;
    let twice = (n % d) << 1;
    if twice > *d || (twice == *d && q.bit(0)) {
        q + 1u8
    } else {
        q
    }
}
/// Nearest binary64 bit pattern, ties to even (positive magnitude).
fn binary(n: &BigUint, d: &BigUint) -> u64 {
    let mut exponent = n.bits() as i64 - d.bits() as i64;
    let below = if exponent >= 0 {
        n < &(d << exponent as usize)
    } else {
        &(n << (-exponent) as usize) < d
    };
    exponent -= i64::from(below);
    if exponent > 1023 {
        return 0x7ff0_0000_0000_0000;
    }
    let shift = 52 - exponent.max(-1022);
    let mantissa = if shift >= 0 {
        divide_nearest(&(n << shift as usize), d)
    } else {
        divide_nearest(n, &(d << (-shift) as usize))
    };
    let significand =
        u64::try_from(mantissa).expect("rounded binary significand has at most 54 bits");
    (((exponent.max(-1022) + 1022) as u64) << 52) + significand
}
fn ratio(digits: BigUint, exp: i64) -> (BigUint, BigUint) {
    if exp >= 0 {
        (digits * pow10(exp), 1u8.into())
    } else {
        (digits, pow10(-exp))
    }
}
fn float_spelling(raw: &str) -> String {
    let sign = if raw.starts_with('-') { "-" } else { "" };
    let raw = raw.trim_start_matches('-');
    let (mantissa, exp) = raw.split_once(['e', 'E']).unwrap_or((raw, "0"));
    let exp = exp.parse::<i64>().unwrap_or(if exp.starts_with('-') {
        i64::MIN
    } else {
        i64::MAX
    });
    let (whole, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = [whole, frac].concat();
    let digits = digits.trim_start_matches('0');
    let dec_exp = exp.saturating_sub(frac.len() as i64);
    let magnitude = dec_exp.saturating_add(digits.len() as i64 - 1);
    if digits.is_empty() || magnitude < -324 {
        return format!("{sign}0.0");
    }
    if magnitude > 308 {
        return format!("{sign}Infinity");
    }
    let (n, d) = ratio(
        BigUint::parse_bytes(digits.as_bytes(), 10).unwrap(),
        dec_exp,
    );
    let bits = binary(&n, &d);
    if bits == 0 {
        return format!("{sign}0.0");
    }
    if bits == 0x7ff0_0000_0000_0000 {
        return format!("{sign}Infinity");
    }
    let exponent = (bits >> 52) as i64;
    let fraction = bits & ((1u64 << 52) - 1);
    let significand = if exponent == 0 {
        fraction
    } else {
        fraction + (1u64 << 52)
    };
    let shift = exponent.max(1) - 1023 - 52;
    let (n, d) = if shift >= 0 {
        (
            BigUint::from(significand) << shift as usize,
            BigUint::from(1u8),
        )
    } else {
        (
            BigUint::from(significand),
            BigUint::from(1u8) << (-shift) as usize,
        )
    };
    let mut magnitude = n.to_str_radix(10).len() as i64 - d.to_str_radix(10).len() as i64;
    if (if magnitude >= 0 {
        n.cmp(&(&d * pow10(magnitude)))
    } else {
        (&n * pow10(-magnitude)).cmp(&d)
    }) == Ordering::Less
    {
        magnitude -= 1;
    }
    for precision in 1..=17 {
        let place = magnitude - precision + 1;
        let (a, b) = if place < 0 {
            (&n * pow10(-place), d.clone())
        } else {
            (n.clone(), &d * pow10(place))
        };
        let nearest = divide_nearest(&a, &b);
        // At a power of two the interval is asymmetric: the nearest candidate can
        // miss on its narrow side while the adjacent one fits on the wide side.
        let candidates = [nearest.clone(), &nearest + 1u8, &nearest - 1u8];
        for candidate in candidates {
            let (x, y) = ratio(candidate.clone(), place);
            if x == BigUint::ZERO || binary(&x, &y) != bits {
                continue;
            }
            let digits = candidate.to_str_radix(10);
            let adj = place + digits.len() as i64 - 1;
            let sig = digits.trim_end_matches('0');
            let mut result = sign.to_owned();
            if !(-4..16).contains(&adj) {
                result.push_str(&sig[..1]);
                if sig.len() > 1 {
                    result.push('.');
                    result.push_str(&sig[1..]);
                }
                write!(result, "e{adj:+03}").unwrap();
            } else if adj < 0 {
                result.push_str("0.");
                result.extend(std::iter::repeat_n('0', (-adj - 1) as usize));
                result.push_str(sig);
            } else {
                let point = adj as usize + 1;
                if point >= sig.len() {
                    result.push_str(sig);
                    result.extend(std::iter::repeat_n('0', point - sig.len()));
                    result.push_str(".0");
                } else {
                    result.push_str(&sig[..point]);
                    result.push('.');
                    result.push_str(&sig[point..]);
                }
            }
            return result;
        }
    }
    unreachable!("17 decimal digits distinguish every binary64")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn digest_spelling_covers_the_binary_exponent_range() {
        // SHA-256 of the same newline-terminated compact json.dumps numbers.
        let mut h = sha2::Sha256::new();
        for exponent in (-348..311).step_by(3) {
            for digits in [
                "17976931348623157",
                "49999999999999995",
                "22250738585072014",
                "1234567890000000000000001",
            ] {
                h.update(float_spelling(&format!("{digits}e{exponent}")));
                h.update(b"\n");
            }
        }
        assert_eq!(
            format!("{:x}", h.finalize()),
            "df129a713434f206b282e8606c577ce88eb41ed04b18f9132af09dc1babe3549"
        );
    }
    #[test]
    fn historical_digest_numbers_and_unicode() {
        for (wire, canonical) in [
            ("1e0", "1.0"),
            ("1.2300", "1.23"),
            ("9007199254740993.0", "9007199254740992.0"),
            ("1e-5", "1e-05"),
            ("1e-4", "0.0001"),
            ("2e16", "2e+16"),
            ("-1e-400", "-0.0"),
            ("5e-324", "5e-324"),
            ("1e309", "Infinity"),
            ("1.00000000000000000000001", "1.0"),
        ] {
            assert_eq!(float_spelling(wire), canonical);
        }
        let mut s = String::new();
        string("😀\u{7f}\n", &mut s);
        assert_eq!(s, r#""\ud83d\ude00\u007f\n""#);
    }
}
