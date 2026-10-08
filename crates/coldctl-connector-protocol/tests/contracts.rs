use coldctl_connector_protocol::{capability::*, model::*, wire::*, *};
use std::collections::{BTreeMap, BTreeSet};
fn mysql() -> Batch {
    serde_json::from_str(include_str!("fixtures/mysql_unsigned.json")).unwrap()
}
fn hello() -> Hello {
    Hello {
        protocol: CURRENT,
        connector: ConnectorPin {
            id: "postgres".into(),
            version: "0.1.1".into(),
            sha256: "a".repeat(64),
        },
        features: BTreeSet::from(["bounded-batches".into()]),
        required_host_features: BTreeSet::new(),
        capabilities: Capabilities {
            source: Some(SourceCapabilities {
                analyze: true,
                encodings: BTreeSet::from(["postgres-text-v2".into()]),
                cursor_versions: BTreeSet::from([1]),
            }),
            sink: None,
            restore: None,
        },
    }
}
#[test]
fn negotiation_rejects_incompatible_major_and_both_missing_feature_directions() {
    let mut h = hello();
    assert_eq!(
        negotiate(&h, &BTreeSet::new(), &h.features).unwrap(),
        CURRENT
    );
    h.protocol.major = 2;
    assert!(negotiate(&h, &BTreeSet::new(), &BTreeSet::new()).is_err());
    h.protocol.major = 1;
    h.protocol.minor = 7;
    assert_eq!(
        negotiate(&h, &BTreeSet::new(), &BTreeSet::new()).unwrap(),
        CURRENT
    );
    h.required_host_features.insert("unsupported".into());
    assert!(negotiate(&h, &BTreeSet::new(), &BTreeSet::new()).is_err());
    h.required_host_features.clear();
    assert!(negotiate(&h, &BTreeSet::new(), &BTreeSet::from(["absent".into()])).is_err());
}
#[test]
fn unsigned_keys_and_decimal_values_survive_json_exactly() {
    let batch = mysql();
    batch.validate().unwrap();
    let json = serde_json::to_vec(&batch).unwrap();
    let again: Batch = serde_json::from_slice(&json).unwrap();
    assert!(batch == again);
    Key::Unsigned(u64::MAX.to_string()).validate().unwrap();
    for s in ["18446744073709551616", "-1", "01", "1e10"] {
        assert!(Key::Unsigned(s.into()).validate().is_err());
    }
    for s in ["9223372036854775808", "-9223372036854775809", "+1", "-0"] {
        assert!(Key::Signed(s.into()).validate().is_err());
    }
}
#[test]
fn raw_bson_round_trips_without_json_normalization() {
    let batch: Batch = serde_json::from_str(include_str!("fixtures/mongodb_bson.json")).unwrap();
    batch.validate().unwrap();
    let BatchPayload::Bson { documents } = &batch.payload else {
        panic!()
    };
    let doc = &documents[0];
    assert!(doc.windows(8).any(|b| b == i64::MAX.to_le_bytes()));
    assert!(doc.windows(3).any(|b| b == [0, 255, 1]));
    assert!(doc.windows(15).any(|b| b == b"\x0aexplicit_null\0"));
    assert!(!doc.windows(7).any(|b| b == b"missing"));
    let copy: Batch = serde_json::from_slice(&serde_json::to_vec(&batch).unwrap()).unwrap();
    assert!(copy == batch);
    let bad = BatchPayload::Bson {
        documents: vec![vec![6, 0, 0, 0, 0]],
    };
    assert!(bad.validate().is_err());
}
#[test]
fn schema_nullability_width_and_size_are_checked() {
    let mut batch = mysql();
    if let BatchPayload::Rows { rows, .. } = &mut batch.payload {
        rows[0][0] = Value::Null;
    }
    assert!(batch.validate().is_err());
    let mut batch = mysql();
    if let BatchPayload::Rows { rows, .. } = &mut batch.payload {
        rows[0].pop();
    }
    assert!(batch.validate().is_err());
    let mut batch = mysql();
    if let BatchPayload::Rows { rows, .. } = &mut batch.payload {
        rows[0][1] = Value::Decimal("9".repeat(65537));
    }
    assert!(batch.validate().is_err());
    let mut batch = mysql();
    batch.next_cursor.token = vec![0; MAX_CURSOR_BYTES + 1];
    assert!(batch.validate().is_err());
}
#[test]
fn control_decode_is_bounded_and_errors_omit_input() {
    let bad = br#"{"private":"DO_NOT_ECHO"}"#;
    assert!(
        !decode_control::<Hello>(bad)
            .unwrap_err()
            .to_string()
            .contains("DO_NOT_ECHO")
    );
    assert!(decode_control::<Hello>(&vec![b' '; MAX_CONTROL_BYTES + 1]).is_err());
    let mut value = serde_json::to_value(hello()).unwrap();
    value["extra"] = true.into();
    assert!(decode_control::<Hello>(&serde_json::to_vec(&value).unwrap()).is_err());
    let h = hello();
    assert_eq!(
        decode_control::<Hello>(&serde_json::to_vec(&h).unwrap()).unwrap(),
        h
    );
}
#[test]
fn config_keeps_references_unresolved_and_rejects_invalid_env_names() {
    let mut c = ConnectorConfig {
        connector_id: "mysql".into(),
        config_version: 1,
        settings: BTreeMap::from([("host".into(), "localhost".into())]),
        secrets: BTreeMap::from([(
            "password".into(),
            SecretRef {
                environment: "UNSET_TEST_REFERENCE".into(),
            },
        )]),
    };
    c.validate().unwrap();
    c.secrets.get_mut("password").unwrap().environment = "1INVALID".into();
    assert!(c.validate().is_err());
    c.connector_id = "../executable".into();
    assert!(c.validate().is_err());
}
#[test]
fn request_requires_bounded_deadline_and_structured_body() {
    let mut r = Request {
        request_id: 1,
        session_id: "session-1".into(),
        operation: Operation::ReadBatch,
        deadline_ms: 10000,
        body: serde_json::json!({"credit_rows":100}),
    };
    r.validate().unwrap();
    r.deadline_ms = 0;
    assert!(r.validate().is_err());
    r.deadline_ms = 10000;
    r.body = serde_json::Value::Null;
    assert!(r.validate().is_err());
}

#[test]
fn frame_header_rejects_bad_lengths_versions_and_reserved_bytes_before_allocation() {
    let h = FrameHeader {
        kind: FrameKind::Data,
        request_id: u64::MAX,
        payload_bytes: MAX_CHUNK_BYTES,
    };
    assert_eq!(FrameHeader::decode(&h.encode().unwrap()).unwrap(), h);
    let mut bytes = h.encode().unwrap();
    bytes[20..24].copy_from_slice(&u32::MAX.to_be_bytes());
    assert!(FrameHeader::decode(&bytes).is_err());
    for index in [0, 5, 7, 8, 9] {
        let mut bytes = h.encode().unwrap();
        bytes[index] = 255;
        assert!(FrameHeader::decode(&bytes).is_err());
    }
    assert!(FrameHeader::decode(&[0; 23]).is_err());
    assert!(
        FrameHeader {
            payload_bytes: 0,
            ..h
        }
        .encode()
        .is_err()
    );
}

#[test]
fn version_intersection_selects_lower_minor() {
    assert_eq!(
        negotiate_versions(
            ProtocolVersion { major: 1, minor: 3 },
            ProtocolVersion { major: 1, minor: 2 }
        )
        .unwrap(),
        ProtocolVersion { major: 1, minor: 2 }
    );
    assert!(
        negotiate_versions(
            ProtocolVersion { major: 1, minor: 0 },
            ProtocolVersion { major: 2, minor: 0 }
        )
        .is_err()
    );
}
