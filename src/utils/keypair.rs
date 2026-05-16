use std::path::Path;

use libp2p::identity::Keypair;

pub fn load_keypair_from(
  path: &Path,
  filename: &str,
) -> anyhow::Result<Keypair> {
  let key_path = path.join(format!("{}.key", filename));
  let pub_key_path = path.join(format!("{}.key.pub", filename));

  Ok(if key_path.exists() {
    Keypair::from_protobuf_encoding(&std::fs::read(key_path)?)?
  } else {
    let keypair = Keypair::generate_ed25519();
    std::fs::write(key_path, keypair.to_protobuf_encoding()?)?;
    std::fs::write(pub_key_path, keypair.public().encode_protobuf())?;
    keypair
  })
}
