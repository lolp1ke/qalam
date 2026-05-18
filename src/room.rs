// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use libp2p::{PeerId, gossipsub::IdentTopic};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::time::QalamTime;

#[derive(Debug)]
#[derive(Clone, Copy)]
#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub struct RoomId(pub [u8; 32]);
impl RoomId {
  pub fn room(name: &str) -> Self {
    let hash = Sha256::digest(format!("room:{}", name));
    Self(hash.into())
  }
  pub fn dm(a: &PeerId, b: &PeerId) -> Self {
    let mut ids = [a.to_bytes(), b.to_bytes()];
    ids.sort();
    let hash = Sha256::new()
      .chain_update(b"dm:")
      .chain_update(&ids[0])
      .chain_update(&ids[1])
      .finalize();
    Self(hash.into())
  }

  pub(crate) fn to_topic(self) -> IdentTopic {
    IdentTopic::new(self.as_str())
  }
  pub fn as_str(&self) -> String {
    format!("room/{}", hex::encode(self.0))
  }
}

#[derive(Debug)]
pub struct Room {
  pub id: RoomId,
  pub members: Vec<PeerId>,
  pub messages: Vec<ChatMessage>,
}

#[derive(Debug)]
pub struct ChatMessage {
  pub id: Uuid,
  pub from: PeerId,
  pub content: Arc<[Arc<str>]>,
  pub ts: QalamTime,
}
