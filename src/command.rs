use std::sync::Arc;

use crate::room::RoomId;

#[derive(Debug)]
pub enum Command {
  JoinRoom { name: String },
  LeaveRoom { room: RoomId },
  SendRoomMessage { room: RoomId, message: Arc<str> },
}
