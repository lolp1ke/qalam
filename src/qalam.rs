use std::time::Duration;

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

use crate::{command::Command, event::Event, room::RoomId};

#[derive(derive_more::Debug)]
pub struct Qalam {
  #[debug(skip)]
  swarm: Swarm<QalamBehaviour>,

  cmd_rx: mpsc::Receiver<Command>,

  event_tx: mpsc::Sender<Event>,

  local_peer_id: PeerId,
}
impl Qalam {
  pub fn new(
    cmd_rx: mpsc::Receiver<Command>,
    event_tx: mpsc::Sender<Event>,
    keypair: Keypair,
    listen_on: Multiaddr,
    bootstrap_nodes: Vec<Multiaddr>,
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

    for addr in bootstrap_nodes.into_iter() {
      if let Err(err) = swarm.dial(addr) {
        tracing::warn!("failed to connect to a bootstrap node: {:?}", err);
      };
    }

    Self {
      swarm,
      cmd_rx,
      event_tx,
      local_peer_id,
    }
  }

  pub async fn start(mut self) {
    loop {
      tokio::select! {
        Some(event) = self.swarm.next() => {
          self.handle_swarm_event(event);
        }
        Some(cmd) = self.cmd_rx.recv() => {
          self.handle_command(cmd).await;
        }
      }
    }
  }

  fn handle_swarm_event(&mut self, event: SwarmEvent<QalamBehaviourEvent>) {
    match event {
      SwarmEvent::Behaviour(QalamBehaviourEvent::Mdns(
        mdns::Event::Discovered(peers),
      )) => {
        for (peer_id, addr) in peers.into_iter() {
          tracing::info!("discovered {}:{}", peer_id, addr);
          self.swarm.add_peer_address(peer_id, addr.clone());
          self.swarm.add_external_address(addr);
        }
      }
      SwarmEvent::Behaviour(QalamBehaviourEvent::Mdns(
        mdns::Event::Expired(peers),
      )) => {
        for (peer_id, addr) in peers.into_iter() {
          tracing::info!("expired {}:{}", peer_id, addr);
          self.swarm.remove_external_address(&addr);
          if self.swarm.disconnect_peer_id(peer_id).is_err() {
            tracing::warn!("failed to disconnect: {}", peer_id);
          };
        }
      }

      _ => {}
    };
  }
  async fn handle_command(&mut self, cmd: Command) {
    match cmd {
      Command::JoinRoom { name } => {
        let room_id = RoomId::room(&name);
        let topic = IdentTopic::new(room_id.as_str());

        if let Err(err) = self.swarm.behaviour_mut().gossipsub.subscribe(&topic)
        {
          tracing::warn!("failed to subscribe to: {}\n{:?}", topic, err);
          return;
        };

        if let Err(err) = self
          .event_tx
          .send(Event::RoomJoined { room: room_id })
          .await
        {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err)
        };
      }

      Command::LeaveRoom { room } => {
        let topic = IdentTopic::new(room.as_str());
        if !self.swarm.behaviour_mut().gossipsub.unsubscribe(&topic) {
          tracing::warn!("failed to unsubscribe from topic: {}", topic);
          return;
        };
        if let Err(err) = self.event_tx.send(Event::RoomLeft { room }).await {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err);
        };
      }

      _ => {}
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
