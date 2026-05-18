// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use libp2p::PeerId;

use crate::room::RoomId;

#[derive(Debug)]
pub enum QalamCommand {
  Shutdown,
  JoinRoom {
    name: Arc<str>,
  },
  LeaveRoom {
    room: RoomId,
  },
  SendRoomMessage {
    room: RoomId,
    from: PeerId,
    message: Arc<[Arc<str>]>,
  },
  RequestPersona {
    peer: PeerId,
  },
}
