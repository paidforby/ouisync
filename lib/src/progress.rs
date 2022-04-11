use std::{
    fmt,
    ops::{Div, Mul},
};

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub(crate) struct Progress {
    pub value: u64,
    pub total: u64,
}

impl Progress {
    pub fn percent(self) -> Percent {
        Percent(self)
    }
}

impl Mul<u64> for Progress {
    type Output = Self;

    fn mul(self, rhs: u64) -> Self::Output {
        Self {
            value: self.value * rhs,
            total: self.total * rhs,
        }
    }
}

impl Div<u64> for Progress {
    type Output = Self;

    fn div(self, rhs: u64) -> Self::Output {
        Self {
            value: self.value / rhs,
            total: self.total / rhs,
        }
    }
}

pub(crate) struct Percent(Progress);

impl fmt::Display for Progress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}/{}", self.value, self.total)
    }
}

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ratio = if self.0.total == 0 {
            1.0
        } else {
            self.0.value as f64 / self.0.total as f64
        };

        let precision = f.precision().unwrap_or(0);

        write!(f, "{:1.*}%", precision, 100.0 * ratio)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_percent() {
        assert_eq!(
            format!(
                "{}",
                Progress {
                    value: 0,
                    total: 10
                }
                .percent()
            ),
            "0%"
        );

        assert_eq!(
            format!(
                "{}",
                Progress {
                    value: 1,
                    total: 10
                }
                .percent()
            ),
            "10%"
        );

        assert_eq!(
            format!(
                "{}",
                Progress {
                    value: 2,
                    total: 10
                }
                .percent()
            ),
            "20%"
        );

        assert_eq!(
            format!(
                "{}",
                Progress {
                    value: 10,
                    total: 10
                }
                .percent()
            ),
            "100%"
        );

        assert_eq!(
            format!(
                "{:.1}",
                Progress {
                    value: 5,
                    total: 10
                }
                .percent()
            ),
            "50.0%"
        );

        assert_eq!(
            format!(
                "{:.2}",
                Progress {
                    value: 5,
                    total: 10
                }
                .percent()
            ),
            "50.00%"
        );
    }
}
