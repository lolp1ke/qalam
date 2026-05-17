use std::sync::Arc;

use libp2p::PeerId;

use crate::room::RoomId;

#[derive(Debug)]
pub enum Command {
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
}
