use coldctl_connector_runtime::{Client, Error, transport};
use std::{path::PathBuf, time::Duration};
async fn client() -> Client {
    Client::launch(
        PathBuf::from(env!("CARGO_BIN_EXE_coldctl-test-connector")),
        "fixture",
        "test-session".into(),
        std::collections::BTreeSet::from(["legacy-postgres-v2".into()]),
    )
    .await
    .unwrap()
}
#[tokio::test]
async fn crash_timeout_truncation_and_correlation_fail_closed() {
    for mode in ["crash", "timeout", "truncated", "wrong-id", "wrong-session"] {
        let mut client = client().await;
        assert!(
            client.call::<_, u32>(mode, 300, false).await.is_err(),
            "{mode}"
        );
        assert_eq!(
            client.call::<_, u32>("echo", 300, false).await.unwrap_err(),
            Error::Protocol
        );
    }
}
#[tokio::test]
async fn ambiguous_write_never_authorizes_replay() {
    let mut client = client().await;
    assert_eq!(
        client
            .call::<_, u32>("crash", 1000, true)
            .await
            .unwrap_err(),
        Error::OutcomeUnknown
    );
}
#[tokio::test]
async fn duplicate_terminal_response_cannot_satisfy_next_request() {
    let mut client = client().await;
    assert_eq!(
        client
            .call::<_, u32>("duplicate", 2000, false)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        client
            .call::<_, u32>("echo", 2000, false)
            .await
            .unwrap_err(),
        Error::Protocol
    );
}
#[tokio::test]
async fn stderr_is_drained_without_becoming_protocol_or_log_content() {
    let mut client = client().await;
    assert_eq!(
        client.call::<_, u32>("stderr", 5000, false).await.unwrap(),
        1
    );
}
#[tokio::test]
async fn cancelled_future_poisoned_session_cannot_be_reused() {
    let mut client = client().await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            client.call::<_, u32>("timeout", 10000, false)
        )
        .await
        .is_err()
    );
    assert_eq!(
        client.call::<_, u32>("echo", 100, false).await.unwrap_err(),
        Error::Protocol
    );
    client.terminate().await;
}
#[tokio::test]
async fn binary_chunks_round_trip_under_backpressure() {
    let (mut writer, mut reader) = tokio::io::duplex(73);
    let value = "雪".repeat(400000);
    let expected = value.clone();
    let send = tokio::spawn(async move { transport::send(&mut writer, 42, &value).await.unwrap() });
    let got: String = transport::receive(&mut reader, 42).await.unwrap();
    assert_eq!(got, expected);
    send.await.unwrap();
}
#[tokio::test]
async fn invalid_headers_and_oversized_metadata_are_rejected() {
    use coldctl_connector_protocol::wire::{FrameHeader, FrameKind};
    use tokio::io::AsyncWriteExt;
    for bytes in [vec![0u8; 24], {
        let meta=br#"{"bytes":67108865,"chunks":65,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#;
        let mut bytes = FrameHeader {
            kind: FrameKind::Control,
            request_id: 1,
            payload_bytes: meta.len() as u32,
        }
        .encode()
        .unwrap()
        .to_vec();
        bytes.extend_from_slice(meta);
        bytes
    }] {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        writer.write_all(&bytes).await.unwrap();
        drop(writer);
        assert_eq!(
            transport::receive::<_, String>(&mut reader, 1)
                .await
                .unwrap_err(),
            Error::Protocol
        );
    }
}

#[tokio::test]
async fn executable_identity_must_match_before_configuration() {
    let result = Client::launch(
        PathBuf::from(env!("CARGO_BIN_EXE_coldctl-test-connector")),
        "wrong-connector",
        "test".into(),
        std::collections::BTreeSet::from(["legacy-postgres-v2".into()]),
    )
    .await;
    assert!(matches!(result, Err(Error::Protocol)));
}

#[tokio::test]
async fn corrupted_payload_hash_is_rejected() {
    use coldctl_connector_protocol::wire::{FrameHeader, FrameKind};
    use tokio::io::AsyncWriteExt;
    let (mut writer, mut reader) = tokio::io::duplex(4096);
    let metadata=br#"{"bytes":4,"chunks":1,"sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#;
    for (kind, bytes) in [
        (FrameKind::Control, metadata.as_slice()),
        (FrameKind::Data, b"null".as_slice()),
    ] {
        writer
            .write_all(
                &FrameHeader {
                    kind,
                    request_id: 1,
                    payload_bytes: bytes.len() as u32,
                }
                .encode()
                .unwrap(),
            )
            .await
            .unwrap();
        writer.write_all(bytes).await.unwrap();
    }
    drop(writer);
    assert_eq!(
        transport::receive::<_, serde_json::Value>(&mut reader, 1)
            .await
            .unwrap_err(),
        Error::Protocol
    );
}

#[cfg(windows)]
#[tokio::test]
async fn terminating_session_also_terminates_descendants() {
    use windows_sys::Win32::{
        Foundation::CloseHandle,
        System::Threading::{OpenProcess, PROCESS_SYNCHRONIZE, WaitForSingleObject},
    };
    let mut client = client().await;
    let pid: u32 = client.call("descendant", 5000, false).await.unwrap();
    // Hold the process handle before termination so PID reuse cannot confuse the check.
    let process = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
    assert!(!process.is_null());
    client.terminate().await;
    let status = unsafe { WaitForSingleObject(process, 5000) };
    unsafe {
        CloseHandle(process);
    }
    assert_eq!(status, 0, "descendant did not exit with its session");
}
