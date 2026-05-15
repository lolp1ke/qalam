use crate::room::{ChatMessage, RoomId};

#[derive(Debug)]
pub enum Event {
  RoomJoined { room: RoomId },
  RoomLeft { room: RoomId },
  ChatMessageReceieved(ChatMessage),
}
