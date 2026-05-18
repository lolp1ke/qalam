// SPDX-License-Identifier: Apache-2.0

use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug)]
#[derive(Clone, Copy)]
pub struct QalamTime(pub(crate) u64);
impl QalamTime {
  pub const fn from_millis(millis: u64) -> Self {
    Self(millis)
  }
  pub fn now() -> Self {
    let now = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .expect("failed to get time since `UNIX_EPOCH`");
    let millis = now
      .as_millis()
      .try_into()
      .expect("i'm impressed this soft is still conserved");
    Self(millis)
  }

  pub const fn to_duration(self) -> Duration {
    Duration::from_millis(self.0)
  }
  pub const fn as_millis(self) -> u64 {
    self.0
  }
}
