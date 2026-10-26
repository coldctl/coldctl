use crate::error::Error;
use base64::{Engine, engine::general_purpose::STANDARD};
use ring::signature::KeyPair;
use sha2::{Digest, Sha256};
use std::io::{Seek, Write};

/// Optional gateway enrollment. Secrets remain in the process environment, never in a manifest.
pub(super) fn sign(file: &mut tempfile::NamedTempFile, exported_at: &str) -> Result<(), Error> {
    let binding = std::env::var("COLDCTL_ARCHIVE_BINDING").ok();
    let key = std::env::var("COLDCTL_ARCHIVE_SIGNING_SEED").ok();
    if binding.is_none() && key.is_none() {
        return Ok(());
    }
    let invalid = || Error::Archive("manifest signing requires a binding and base64 Ed25519 seed");
    let binding = binding
        .filter(|s| !s.is_empty() && s.len() <= 128)
        .ok_or_else(invalid)?;
    let seed = STANDARD
        .decode(key.ok_or_else(invalid)?)
        .map_err(|_| invalid())?;
    sign_with_key(file, exported_at, &binding, &seed)
}
fn sign_with_key(
    file: &mut tempfile::NamedTempFile,
    exported_at: &str,
    binding: &str,
    seed: &[u8],
) -> Result<(), Error> {
    let invalid = || Error::Archive("invalid manifest signing input");
    let pair = ring::signature::Ed25519KeyPair::from_seed_unchecked(seed).map_err(|_| invalid())?;
    if file.as_file().metadata().map_err(|_| invalid())?.len() > 8 * 1024 * 1024 {
        return Err(Error::Archive("signed manifest exceeds gateway size limit"));
    }
    file.as_file_mut().rewind().map_err(|_| invalid())?;
    let mut value: serde_json::Value =
        serde_json::from_reader(file.as_file_mut()).map_err(|_| invalid())?;
    value["gateway_exported_at"] = exported_at.into();
    sort_json(&mut value);
    let canonical = serde_json::to_vec(&value).map_err(|_| invalid())?;
    let hash = format!("{:x}", Sha256::digest(&canonical));
    let message =
        serde_json::to_vec(&("coldctl-manifest-v1", &binding, hash)).map_err(|_| invalid())?;
    value["gateway_signature"] = serde_json::json!({"version":1,"binding":binding,"public_key":STANDARD.encode(pair.public_key().as_ref()),"signature":STANDARD.encode(pair.sign(&message).as_ref())});
    let encoded = serde_json::to_vec(&value).map_err(|_| invalid())?;
    if encoded.len() > 8 * 1024 * 1024 {
        return Err(Error::Archive("signed manifest exceeds gateway size limit"));
    }
    file.as_file_mut().set_len(0).map_err(|_| invalid())?;
    file.as_file_mut().rewind().map_err(|_| invalid())?;
    file.write_all(&encoded).map_err(|_| invalid())?;
    Ok(())
}

fn sort_json(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for value in map.values_mut() {
                sort_json(value);
            }
            map.sort_keys();
        }
        serde_json::Value::Array(values) => {
            for value in values {
                sort_json(value);
            }
        }
        _ => {}
    }
}

#[test]
fn signing_matches_independent_ed25519_protocol_vector() {
    let expected: serde_json::Value =
        serde_json::from_slice(include_bytes!("../../tests/manifest-signature-v1.json")).unwrap();
    let mut input = expected.clone();
    input.as_object_mut().unwrap().remove("gateway_signature");
    let mut file = tempfile::NamedTempFile::new().unwrap();
    serde_json::to_writer(&mut file, &input).unwrap();
    sign_with_key(
        &mut file,
        "2026-10-26T00:00:00Z",
        "test-namespace",
        &[7; 32],
    )
    .unwrap();
    file.as_file_mut().rewind().unwrap();
    let actual: serde_json::Value = serde_json::from_reader(file.as_file_mut()).unwrap();
    assert_eq!(actual, expected);
    assert!(sign_with_key(&mut file, "now", "test-namespace", &[7; 31]).is_err());
}
