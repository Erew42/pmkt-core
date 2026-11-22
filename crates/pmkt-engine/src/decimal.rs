//! An exact signed coefficient and decimal scale. Unrepresentable input is rejected.
use std::{cmp::Ordering, fmt, str::FromStr};

use num_bigint::BigInt;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ScaledDecimal {
    coefficient: i128,
    scale: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DecimalError {
    #[error("invalid decimal")]
    Invalid,
    #[error("decimal exceeds i128 coefficient or 38 decimal places")]
    Overflow,
    #[error("step must be positive")]
    Step,
}

type Result<T> = std::result::Result<T, DecimalError>;

impl ScaledDecimal {
    pub const ZERO: Self = Self {
        coefficient: 0,
        scale: 0,
    };
    pub const ONE: Self = Self {
        coefficient: 1,
        scale: 0,
    };
    pub const MAX_SCALE: u32 = 38;

    pub fn new(mut coefficient: i128, mut scale: u32) -> Result<Self> {
        if coefficient == 0 {
            return Ok(Self::ZERO);
        }
        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        if scale > Self::MAX_SCALE {
            return Err(DecimalError::Overflow);
        }
        Ok(Self { coefficient, scale })
    }

    pub fn from_json(value: &Value) -> Result<Self> {
        match value {
            Value::String(s) => s.parse(),
            Value::Number(n) => n.as_str().parse(),
            _ => Err(DecimalError::Invalid),
        }
    }
    pub const fn coefficient(self) -> i128 {
        self.coefficient
    }
    pub const fn scale(self) -> u32 {
        self.scale
    }
    pub fn is_probability(self) -> bool {
        (Self::ZERO..=Self::ONE).contains(&self)
    }

    pub fn checked_add(self, rhs: Self) -> Result<Self> {
        let scale = self.scale.max(rhs.scale);
        let a = 10_i128.pow(scale - self.scale);
        let b = 10_i128.pow(scale - rhs.scale);
        if let (Some(x), Some(y)) = (
            self.coefficient.checked_mul(a),
            rhs.coefficient.checked_mul(b),
        ) && let Some(sum) = x.checked_add(y)
        {
            return Self::new(sum, scale);
        }
        Self::from_wide(
            BigInt::from(self.coefficient) * a + BigInt::from(rhs.coefficient) * b,
            scale,
        )
    }

    /// One minus a binary price, without complementing an already-rounded quote.
    pub fn complement(self) -> Result<Self> {
        if !self.is_probability() {
            return Err(DecimalError::Invalid);
        }
        Self::new(10_i128.pow(self.scale) - self.coefficient, self.scale)
    }

    fn from_wide(mut value: BigInt, mut scale: u32) -> Result<Self> {
        while scale > 0 && &value % 10u8 == BigInt::ZERO {
            value /= 10;
            scale -= 1;
        }
        Self::new(value.try_into().map_err(|_| DecimalError::Overflow)?, scale)
    }

    /// Round down/up to a multiple of a positive price tick or size increment.
    pub fn floor_to(self, step: Self) -> Result<Self> {
        self.round_to(step, false)
    }
    pub fn ceil_to(self, step: Self) -> Result<Self> {
        self.round_to(step, true)
    }

    fn round_to(self, step: Self, up: bool) -> Result<Self> {
        if step <= Self::ZERO {
            return Err(DecimalError::Step);
        }
        let scale = self.scale.max(step.scale);
        let v = BigInt::from(self.coefficient) * 10_i128.pow(scale - self.scale);
        let unit = BigInt::from(step.coefficient) * 10_i128.pow(scale - step.scale);
        let rem = &v % &unit;
        let mut rounded = v - &rem;
        if up && rem > BigInt::ZERO {
            rounded += unit;
        } else if !up && rem < BigInt::ZERO {
            rounded -= unit;
        }
        Self::from_wide(rounded, scale)
    }
}

impl Ord for ScaledDecimal {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.scale == other.scale {
            return self.coefficient.cmp(&other.coefficient);
        }
        match self.coefficient.signum().cmp(&other.coefficient.signum()) {
            Ordering::Equal => {}
            order => return order,
        }
        // Compare whole parts first. Fractional remainders always fit at the greater scale.
        let parts = |v: &Self| {
            let magnitude = v.coefficient.unsigned_abs();
            let unit = 10_u128.pow(v.scale);
            (
                magnitude / unit,
                (magnitude % unit) * 10_u128.pow(self.scale.max(other.scale) - v.scale),
            )
        };
        let order = parts(self).cmp(&parts(other));
        if self.coefficient < 0 {
            order.reverse()
        } else {
            order
        }
    }
}
impl PartialOrd for ScaledDecimal {
    fn partial_cmp(&self, rhs: &Self) -> Option<Ordering> {
        Some(self.cmp(rhs))
    }
}

impl FromStr for ScaledDecimal {
    type Err = DecimalError;
    fn from_str(text: &str) -> Result<Self> {
        let text = text.trim();
        let (mantissa, exp) = match text.split_once(['e', 'E']) {
            Some((m, e)) => (m, e.parse::<i64>().map_err(|_| DecimalError::Invalid)?),
            None => (text, 0),
        };
        let negative = mantissa.starts_with('-');
        let unsigned = mantissa.strip_prefix(['+', '-']).unwrap_or(mantissa);
        let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        if whole.is_empty() && fraction.is_empty()
            || !whole
                .bytes()
                .chain(fraction.bytes())
                .all(|c| c.is_ascii_digit())
        {
            return Err(DecimalError::Invalid);
        }
        let digits = [whole, fraction].concat();
        let significant = digits.trim_start_matches('0');
        if significant.is_empty() {
            return Ok(Self::ZERO);
        }
        let mantissa = significant.trim_end_matches('0');
        let scale = i128::try_from(fraction.len()).unwrap()
            - i128::from(exp)
            - (significant.len() - mantissa.len()) as i128;
        if scale > i128::from(Self::MAX_SCALE) || scale < -38 {
            return Err(DecimalError::Overflow);
        }
        // Accumulate negative digits directly so i128::MIN is representable.
        let mut coefficient = 0_i128;
        for d in mantissa.bytes() {
            let digit = i128::from(d - b'0');
            coefficient = coefficient
                .checked_mul(10)
                .and_then(|v| v.checked_add(if negative { -digit } else { digit }))
                .ok_or(DecimalError::Overflow)?;
        }
        if scale < 0 {
            coefficient = coefficient
                .checked_mul(10_i128.pow((-scale) as u32))
                .ok_or(DecimalError::Overflow)?;
        }
        Self::new(coefficient, scale.max(0) as u32)
    }
}
impl fmt::Display for ScaledDecimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.scale == 0 {
            return self.coefficient.fmt(f);
        }
        let n = self.coefficient.unsigned_abs();
        let unit = 10_u128.pow(self.scale);
        let sign = if self.coefficient < 0 { "-" } else { "" };
        write!(
            f,
            "{sign}{}.{:0width$}",
            n / unit,
            n % unit,
            width = self.scale as usize
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn d(s: &str) -> ScaledDecimal {
        s.parse().unwrap()
    }
    #[test]
    fn adjacent_deep_prices_and_exponents() {
        let a = d("0.10000000000000000000000000000000000001");
        assert!(d(".1000") < a);
        assert_eq!(a, d("10000000000000000000000000000000000001e-38"));
        assert_eq!(
            a.complement().unwrap().to_string(),
            "0.89999999999999999999999999999999999999"
        );
        assert_eq!(d("0.1").checked_add(d("2e-1")).unwrap(), d(".30"));
        assert!(d("-0.1") > d("-.11"));
    }
    #[test]
    fn range_and_checked_rounding() {
        let max = d(&i128::MAX.to_string());
        assert!(max.checked_add(ScaledDecimal::ONE).is_err());
        assert!(max > d("1e-38"));
        assert_eq!(d(&i128::MIN.to_string()).to_string(), i128::MIN.to_string());
        for s in ["1e39", "1e-39", "1.1.0", "", ".", "NaN", "true"] {
            assert!(s.parse::<ScaledDecimal>().is_err());
        }
        assert_eq!(d("-1.02").floor_to(d(".05")).unwrap(), d("-1.05"));
        assert_eq!(d("1.02").ceil_to(d(".05")).unwrap(), d("1.05"));
        assert!(d("1").ceil_to(d("0")).is_err());
    }
}
