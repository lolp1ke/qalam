// SPDX-License-Identifier: Apache-2.0

use std::path::Path;

use anyhow::bail;
use libp2p::identity::Keypair;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
struct FileContent {
  persona: String,
  key: Vec<u8>,
}

pub fn load_keypair_from(
  path: &Path,
  persona: &str,
) -> anyhow::Result<Keypair> {
  let hashed_persona = Sha256::digest(persona);
  let sanitized_path = sanitize(hashed_persona);
  let key_path = path.join(format!("{}.key", sanitized_path));
  // let pub_key_path = path.join(format!("{}.key.pub", sanitized_path));

  Ok(if key_path.exists() {
    let content = toml::from_slice::<FileContent>(&std::fs::read(key_path)?)?;

    if content.persona != persona {
      bail!("Wrong persona provided");
    };
    // store persona's peer_id to really distinguish between different identities as keypair cannot be altered without creating new persona
    // something like steam's past username thingy
    Keypair::from_protobuf_encoding(&content.key)?
  } else {
    let keypair = Keypair::generate_ed25519();
    let file_content = toml::to_string(&FileContent {
      persona: persona.to_string(),
      key: keypair.to_protobuf_encoding()?,
    })?;
    std::fs::write(key_path, file_content)?;

    // std::fs::write(pub_key_path, keypair.public().encode_protobuf())?;
    keypair
  })
}

fn sanitize<T>(data: T) -> String
where
  T: AsRef<[u8]>,
{
  hex::encode(data)
  // BASE64_URL_SAFE_NO_PAD.encode(input)
}
fn decode_sanitized(data: &str) -> String {
  todo!();
  // let bytes = BASE64_URL_SAFE_NO_PAD.decode(input).unwrap();
  // String::from_utf8(bytes).unwrap()
}
