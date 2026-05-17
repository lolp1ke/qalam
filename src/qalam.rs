use std::{sync::Arc, time::Duration};

use futures_util::StreamExt;
use libp2p::{
  Multiaddr, PeerId, Swarm, SwarmBuilder,
  gossipsub::{self, IdentTopic},
  identity::Keypair,
  mdns, noise,
  swarm::{NetworkBehaviour, SwarmEvent},
  tcp, yamux,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
  command::Command,
  event::Event,
  room::{ChatMessage, RoomId},
};

#[derive(derive_more::Debug)]
pub struct Qalam {
  local_peer_id: PeerId,
  #[debug(skip)]
  swarm: Swarm<QalamBehaviour>,

  cmd_rx: mpsc::UnboundedReceiver<Command>,
  event_tx: mpsc::Sender<Event>,
}
impl Qalam {
  pub fn new(
    cmd_rx: mpsc::UnboundedReceiver<Command>,
    event_tx: mpsc::Sender<Event>,
    keypair: Keypair,
    listen_on: Multiaddr,
    bootstrap_nodes: Option<Vec<Multiaddr>>,
  ) -> Self {
    let local_peer_id = keypair.public().to_peer_id();

    let mut swarm = SwarmBuilder::with_existing_identity(keypair)
      .with_tokio()
      .with_tcp(
        tcp::Config::default(),
        noise::Config::new,
        yamux::Config::default,
      )
      .expect("tcp build failed")
      .with_behaviour(|keypair| {
        QalamBehaviour::new(local_peer_id, keypair.clone())
      })
      .expect("behaviour build failed")
      .build();

    swarm
      .listen_on(listen_on)
      .expect("failed to start listening");

    if let Some(bootstrap_nodes) = bootstrap_nodes {
      for addr in bootstrap_nodes.into_iter() {
        if let Err(err) = swarm.dial(addr) {
          tracing::warn!("failed to connect to a bootstrap node: {:?}", err);
        };
      }
    };

    Self {
      local_peer_id,
      swarm,
      cmd_rx,
      event_tx,
    }
  }

  pub fn local_peer_id(&self) -> PeerId {
    self.local_peer_id
  }

  pub async fn start(mut self) {
    loop {
      tokio::select! {
        Some(event) = self.swarm.next() => {
          self.handle_swarm_event(event).await;
        }
        Some(cmd) = self.cmd_rx.recv() => {
          self.handle_command(cmd).await;
        }
      }
    }
  }

  async fn handle_swarm_event(
    &mut self,
    event: SwarmEvent<QalamBehaviourEvent>,
  ) {
    match event {
      SwarmEvent::Behaviour(QalamBehaviourEvent::Mdns(
        mdns::Event::Discovered(peers),
      )) => {
        for (peer_id, addr) in peers.into_iter() {
          tracing::info!("discovered {}:{}", peer_id, addr);
          self
            .swarm
            .behaviour_mut()
            .gossipsub
            .add_explicit_peer(&peer_id);
          self.swarm.add_peer_address(peer_id, addr.clone());
          self.swarm.add_external_address(addr);
        }
      }
      SwarmEvent::Behaviour(QalamBehaviourEvent::Mdns(
        mdns::Event::Expired(peers),
      )) => {
        for (peer_id, addr) in peers.into_iter() {
          tracing::info!("expired {}:{}", peer_id, addr);
          self
            .swarm
            .behaviour_mut()
            .gossipsub
            .remove_explicit_peer(&peer_id);
          self.swarm.remove_external_address(&addr);
          if self.swarm.disconnect_peer_id(peer_id).is_err() {
            tracing::warn!("failed to disconnect: {}", peer_id);
          };
        }
      }
      SwarmEvent::Behaviour(QalamBehaviourEvent::Gossipsub(
        gossipsub::Event::Message {
          propagation_source,
          message_id,
          message,
        },
      )) => {
        let Ok(content) = String::from_utf8(message.data) else {
          return;
        };
        if let Err(err) = self
          .event_tx
          .send(Event::ChatMessageReceieved(ChatMessage {
            id: Uuid::now_v7(),
            room: RoomId::room("global"),
            from: propagation_source,
            content: content.lines().map(Arc::from).collect(),
            ts: 0,
          }))
          .await
        {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err);
        };
        // tracing::info!("got {:?}", message);
      }
      SwarmEvent::Behaviour(QalamBehaviourEvent::Gossipsub(
        gossipsub::Event::Subscribed { peer_id, topic },
      )) => {
        tracing::info!("peer {:?} subscribed to: {:?}", peer_id, topic);
      }

      _ => {}
    };
  }
  async fn handle_command(&mut self, cmd: Command) {
    match cmd {
      Command::JoinRoom { name } => {
        let room = RoomId::room(&name);
        let topic = room.to_topic();

        if let Err(err) = self.swarm.behaviour_mut().gossipsub.subscribe(&topic)
        {
          tracing::warn!("failed to subscribe to: {}\n{:?}", topic, err);
          return;
        };
        tracing::info!("subscribed to: {:?}", topic);

        if let Err(err) = self.event_tx.send(Event::RoomJoined { room }).await {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err)
        };
      }
      Command::LeaveRoom { room } => {
        let topic = room.to_topic();
        if !self.swarm.behaviour_mut().gossipsub.unsubscribe(&topic) {
          tracing::warn!("failed to unsubscribe from topic: {}", topic);
          return;
        };
        if let Err(err) = self.event_tx.send(Event::RoomLeft { room }).await {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err);
        };
      }

      Command::SendRoomMessage {
        room,
        from,
        message,
      } => {
        let topic = room.to_topic();
        if let Err(err) = self
          .swarm
          .behaviour_mut()
          .gossipsub
          .publish(topic.clone(), message.join("\r\n").into_bytes())
        {
          tracing::warn!("failed to publish to {:?}: {:?}", topic, err);
        };
        if let Err(err) = self
          .event_tx
          .send(Event::ChatMessageReceieved(ChatMessage {
            id: Uuid::now_v7(),
            room,
            from,
            content: message,
            ts: 0,
          }))
          .await
        {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err);
        };
      }
    };
  }
}

#[derive(derive_more::Debug)]
#[derive(NetworkBehaviour)]
pub struct QalamBehaviour {
  gossipsub: gossipsub::Behaviour,
  #[debug(skip)]
  mdns: mdns::tokio::Behaviour,
}
impl QalamBehaviour {
  pub fn new(local_peer_id: PeerId, keypair: Keypair) -> Self {
    let gossipsub_config = gossipsub::ConfigBuilder::default()
      .build()
      .expect("gossipsub config build failed");

    let gossipsub = gossipsub::Behaviour::new(
      gossipsub::MessageAuthenticity::Signed(keypair),
      gossipsub_config,
    )
    .expect("gossipsub build failed");

    let mdns_config = mdns::Config {
      query_interval: Duration::from_secs(5),
      ..Default::default()
    };
    let mdns = mdns::Behaviour::new(mdns_config, local_peer_id)
      .expect("mdns build failed");

    Self { gossipsub, mdns }
  }
}
