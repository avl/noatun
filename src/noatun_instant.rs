use std::fmt::{Debug, Formatter};
use std::ops::{Add, AddAssign, Sub};
use std::time::Duration;

#[cfg(feature = "tokio")]
type Inner = tokio::time::Instant;
#[cfg(not(feature = "tokio"))]
type Inner = std::time::Instant;

/// Noatun Instant.
///
/// This is a simple wrapper around either [`std::time::Instant`], or if tokio support is
/// enabled, `tokio::time::Instant`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Instant(Inner);

impl Debug for Instant {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<std::time::Instant> for Instant {
    fn from(value: std::time::Instant) -> Self {
        #[cfg(feature = "tokio")] {
            Instant(value.into())
        }
        #[cfg(not(feature = "tokio"))] {
            Instant(value)
        }
    }
}

impl Instant {
    /// Return an instant corresponding to the current time
    pub fn now() -> Self {
        Self(Inner::now())
    }
    /// Calculate the positive duration since 'other'. If 'other' is later than
    /// 'self', a zero duration is returned.
    pub fn saturating_duration_since(&self, other: Instant) -> Duration {
        self.0.saturating_duration_since(other.0)
    }
    /// Calculate the amount of time that has elapsed from 'self' until [`Self::now`].
    pub fn elapsed(&self) -> Duration {
        self.0.elapsed()
    }
    /// Convert this noatun instant to a tokio Instant
    #[cfg(feature = "tokio")]
    pub fn tokio_instant(&self) -> Inner {
        self.0
    }
}
impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs;
    }
}
impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, rhs: Instant) -> Self::Output {
        self.0 - rhs.0
    }
}
