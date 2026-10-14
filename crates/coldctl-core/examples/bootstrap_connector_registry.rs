//! One-time offline bootstrap. Never prints private key material.
use base64::Engine;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs::OpenOptions, io::Write, path::Path};

fn canonical(value: &Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut serializer =
        serde_json::Serializer::with_formatter(&mut bytes, olpc_cjson::CanonicalFormatter::new());
    value.serialize(&mut serializer).unwrap();
    bytes
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn public(key: &Ed25519KeyPair) -> (String, Value) {
    let value = json!({"keytype":"ed25519","scheme":"ed25519","keyval":{"public":hex(key.public_key().as_ref())}});
    (hex(&Sha256::digest(canonical(&value))), value)
}
fn write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err(
            "usage: bootstrap_connector_registry NEW_PRIVATE_DIRECTORY ROOT_EXPIRY_RFC3339".into(),
        );
    }
    let directory = Path::new(&args[0]);
    #[cfg_attr(not(unix), allow(unused_mut))]
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(directory)?; // Refuse overwrite, including symlinks.
    let rng = ring::rand::SystemRandom::new();
    let root_der = Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| "key generation failed")?;
    let online_der = Ed25519KeyPair::generate_pkcs8(&rng).map_err(|_| "key generation failed")?;
    let root = Ed25519KeyPair::from_pkcs8(root_der.as_ref()).map_err(|_| "invalid key")?;
    let online = Ed25519KeyPair::from_pkcs8(online_der.as_ref()).map_err(|_| "invalid key")?;
    let (root_id, root_public) = public(&root);
    let (online_id, online_public) = public(&online);
    let signed = json!({"_type":"root","spec_version":"1.0.0","version":1,"expires":args[1],"consistent_snapshot":false,
        "keys":{root_id.clone():root_public,online_id.clone():online_public},
        "roles":{"root":{"keyids":[root_id],"threshold":1},"targets":{"keyids":[online_id],"threshold":1},"snapshot":{"keyids":[online_id],"threshold":1},"timestamp":{"keyids":[online_id],"threshold":1}}});
    let envelope = json!({"signatures":[{"keyid":root_id,"sig":hex(root.sign(&canonical(&signed)).as_ref())}],"signed":signed});
    let bytes = canonical(&envelope);
    let checked: tough::schema::Signed<tough::schema::Root> = serde_json::from_slice(&bytes)?;
    checked.signed.verify_role(&checked)?;
    for (name, der) in [
        ("root.private.pem", root_der.as_ref()),
        ("online.private.pem", online_der.as_ref()),
    ] {
        let encoded = base64::engine::general_purpose::STANDARD.encode(der);
        let pem = format!("-----BEGIN PRIVATE KEY-----\n{encoded}\n-----END PRIVATE KEY-----\n");
        write(&directory.join(name), pem.as_bytes())?;
    }
    write(&directory.join("root.json"), &bytes)?;
    println!("Public root SHA256: {}", hex(&Sha256::digest(&bytes)));
    println!(
        "Keep root.private.pem offline. Only online.private.pem belongs in the GitHub signing secret."
    );
    Ok(())
}
