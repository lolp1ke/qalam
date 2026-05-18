// SPDX-License-Identifier: Apache-2.0

use std::{
  collections::{BTreeMap, HashMap},
  sync::Arc,
  time::Duration,
};

use futures_util::StreamExt;
use libp2p::{
  Multiaddr, PeerId, Swarm, SwarmBuilder,
  gossipsub::{self, TopicHash},
  identity::Keypair,
  mdns, noise, request_response,
  swarm::{NetworkBehaviour, SwarmEvent},
  tcp, yamux,
};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{
  command::QalamCommand,
  direct::{
    QalamDirect, QalamDirectProtocol, QalamDirectRequest, QalamDirectResponse,
  },
  event::QalamEvent,
  room::{ChatMessage, RoomId},
  time::QalamTime,
};

#[derive(derive_more::Debug)]
pub struct Qalam {
  persona: Arc<str>,
  local_peer_id: PeerId,
  #[debug(skip)]
  swarm: Swarm<QalamBehaviour>,

  cmd_rx: mpsc::UnboundedReceiver<QalamCommand>,
  event_tx: mpsc::Sender<QalamEvent>,

  room_id_by_topic: BTreeMap<TopicHash, RoomId>,
}
impl Qalam {
  pub fn new(
    persona: Arc<str>,
    cmd_rx: mpsc::UnboundedReceiver<QalamCommand>,
    event_tx: mpsc::Sender<QalamEvent>,
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
      persona,
      local_peer_id,
      swarm,
      cmd_rx,
      event_tx,
      room_id_by_topic: BTreeMap::default(),
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
          if !self.handle_command(cmd).await {
            break;
          };
        }
      }
    }
  }

  async fn handle_swarm_event(
    &mut self,
    event: SwarmEvent<QalamBehaviourEvent>,
  ) {
    match event {
      SwarmEvent::Behaviour(event) => {
        if let Err(err) = self.handle_behaviour(event).await {
          tracing::warn!("behaviour error: {:?}", err);
        };
      }

      event => {
        tracing::trace!("swarm event: {:?}", event);
      }
    };
  }
  async fn handle_behaviour(
    &mut self,
    event: QalamBehaviourEvent,
  ) -> anyhow::Result<()> {
    match event {
      QalamBehaviourEvent::Mdns(mdns::Event::Discovered(peers)) => {
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
      QalamBehaviourEvent::Mdns(mdns::Event::Expired(peers)) => {
        for (peer_id, addr) in peers.into_iter() {
          tracing::info!("expired {}:{}", peer_id, addr);
          self
            .swarm
            .behaviour_mut()
            .gossipsub
            .remove_explicit_peer(&peer_id);
          self.swarm.remove_external_address(&addr);
          if self.swarm.disconnect_peer_id(peer_id).is_err() {
            tracing::warn!("failed to disconnect from peer");
          };
        }
      }

      QalamBehaviourEvent::Gossipsub(gossipsub::Event::Message {
        propagation_source,
        message,
        ..
      }) => {
        let content = String::from_utf8(message.data)?;
        if let Some(&room) = self.room_id_by_topic.get(&message.topic) {
          self
            .event_tx
            .send(QalamEvent::ChatMessageReceieved {
              room,
              message: ChatMessage {
                id: Uuid::now_v7(),
                from: propagation_source,
                content: content.lines().map(Arc::from).collect(),
                ts: QalamTime::now(),
              },
            })
            .await?;
        };
      }
      QalamBehaviourEvent::Gossipsub(gossipsub::Event::Subscribed {
        peer_id,
        topic,
      }) => {
        tracing::info!("peer {:?} subscribed to: {:?}", peer_id, topic);
      }
      QalamBehaviourEvent::Direct(request_response::Event::Message {
        message:
          request_response::Message::Request {
            request, channel, ..
          },
        ..
      }) => {
        let response = self.handle_direct_request(request.clone()).await;
        self
          .swarm
          .behaviour_mut()
          .direct
          .send_response(channel, response)?;
      }
      QalamBehaviourEvent::Direct(request_response::Event::Message {
        message: request_response::Message::Response { response, .. },
        ..
      }) => {
        self.handle_direct_response(response).await?;
      }

      _ => {}
    };

    Ok(())
  }
  async fn handle_direct_request(
    &mut self,
    request: QalamDirectRequest,
  ) -> QalamDirectResponse {
    match request {
      QalamDirectRequest::Ping => QalamDirectResponse::Pong,
      QalamDirectRequest::Persona => QalamDirectResponse::Persona {
        peer: self.local_peer_id,
        ident: self.persona.clone(),
      },
    }
  }
  async fn handle_direct_response(
    &mut self,
    response: QalamDirectResponse,
  ) -> anyhow::Result<()> {
    match response {
      QalamDirectResponse::Pong => {
        tracing::info!("pong");
      }
      QalamDirectResponse::Persona {
        peer,
        ident: persona,
      } => {
        tracing::debug!("fires");
        self
          .event_tx
          .send(QalamEvent::PersonaReceived { peer, persona })
          .await?;
      }
    };
    Ok(())
  }
  async fn handle_command(&mut self, cmd: QalamCommand) -> bool {
    match cmd {
      QalamCommand::Shutdown => {
        return false;
      }
      QalamCommand::JoinRoom { name } => {
        let room = RoomId::room(&name);
        let topic = room.to_topic();

        if let Err(err) = self.swarm.behaviour_mut().gossipsub.subscribe(&topic)
        {
          tracing::warn!("failed to subscribe to: {}\n{:?}", topic, err);
          return true;
        };
        tracing::info!("subscribed to: {:?}", topic);
        self.room_id_by_topic.insert(topic.hash(), room);

        if let Err(err) =
          self.event_tx.send(QalamEvent::RoomJoined { room }).await
        {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err)
        };
      }
      QalamCommand::LeaveRoom { room } => {
        let topic = room.to_topic();
        if !self.swarm.behaviour_mut().gossipsub.unsubscribe(&topic) {
          tracing::warn!("failed to unsubscribe from topic: {}", topic);
          return true;
        };
        if self.room_id_by_topic.remove(&topic.hash()).is_none() {
          tracing::warn!("double remove of room in [`room_id_by_name`]");
        };
        if let Err(err) =
          self.event_tx.send(QalamEvent::RoomLeft { room }).await
        {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err);
        };
      }

      QalamCommand::SendRoomMessage {
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
          .send(QalamEvent::ChatMessageReceieved {
            room,
            message: ChatMessage {
              id: Uuid::now_v7(),
              from,
              content: message,
              ts: QalamTime::now(),
            },
          })
          .await
        {
          tracing::warn!("failed to send event: {:?}\n{:?}", err.0, err);
        };
      }

      QalamCommand::RequestPersona { peer } => {
        self
          .swarm
          .behaviour_mut()
          .direct
          .send_request(&peer, QalamDirectRequest::Persona);
      }
    };
    true
  }
}

#[derive(derive_more::Debug)]
#[derive(NetworkBehaviour)]
pub struct QalamBehaviour {
  gossipsub: gossipsub::Behaviour,
  #[debug(skip)]
  mdns: mdns::tokio::Behaviour,
  #[debug(skip)]
  direct: request_response::Behaviour<QalamDirect>,
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

    Self {
      gossipsub,
      mdns,
      direct: request_response::Behaviour::new(
        [(QalamDirectProtocol, request_response::ProtocolSupport::Full)],
        request_response::Config::default(),
      ),
    }
  }
}
