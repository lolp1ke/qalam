// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use libp2p::PeerId;

use crate::room::{ChatMessage, RoomId};

#[derive(Debug)]
pub enum QalamEvent {
  RoomJoined { room: RoomId },
  RoomLeft { room: RoomId },
  ChatMessageReceieved { room: RoomId, message: ChatMessage },
  PersonaReceived { peer: PeerId, persona: Arc<str> },
}
