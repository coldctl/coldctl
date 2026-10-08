//! Fault-injection executable for integration tests; never installed as a connector.
use coldctl_connector_protocol::{capability::*, model::ConnectorPin};
use coldctl_connector_runtime::{Call, Reply, transport};
use std::collections::BTreeSet;
use tokio::io::AsyncWriteExt;
#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("--wait") {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        return;
    }
    let mut output = tokio::io::stdout();
    let mut input = tokio::io::stdin();
    let features = BTreeSet::from(["legacy-postgres-v2".into()]);
    let hello = Hello {
        protocol: CURRENT,
        connector: ConnectorPin {
            id: "fixture".into(),
            version: "1".into(),
            sha256: coldctl_connector_runtime::digest(&std::env::current_exe().unwrap()).unwrap(),
        },
        features: features.clone(),
        required_host_features: features,
        capabilities: Capabilities {
            source: Some(SourceCapabilities {
                analyze: false,
                encodings: BTreeSet::from(["fixture".into()]),
                cursor_versions: BTreeSet::from([1]),
            }),
            sink: None,
            restore: None,
        },
    };
    transport::send(&mut output, 0, &hello).await.unwrap();
    let call: Call<String> = transport::receive(&mut input, 1).await.unwrap();
    match call.body.as_str() {
        "descendant" => {
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--wait")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap();
            transport::send(
                &mut output,
                1,
                &Reply {
                    session: call.session,
                    result: Ok(child.id()),
                },
            )
            .await
            .unwrap();
            let _ = child.wait();
        }
        "crash" => std::process::exit(4),
        "timeout" => tokio::time::sleep(std::time::Duration::from_secs(60)).await,
        "truncated" => {
            output.write_all(b"CCTL").await.unwrap();
        }
        "wrong-session" => {
            transport::send(
                &mut output,
                1,
                &Reply {
                    session: "wrong".into(),
                    result: Ok(1u32),
                },
            )
            .await
            .unwrap();
        }
        "wrong-id" => {
            transport::send(
                &mut output,
                2,
                &Reply {
                    session: call.session,
                    result: Ok(1u32),
                },
            )
            .await
            .unwrap();
        }
        "duplicate" => {
            let reply = Reply {
                session: call.session,
                result: Ok(1u32),
            };
            transport::send(&mut output, 1, &reply).await.unwrap();
            transport::send(&mut output, 1, &reply).await.unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
        "stderr" => {
            let mut error = tokio::io::stderr();
            error.write_all(&vec![b'x'; 2 * 1024 * 1024]).await.unwrap();
            transport::send(
                &mut output,
                1,
                &Reply {
                    session: call.session,
                    result: Ok(1u32),
                },
            )
            .await
            .unwrap();
        }
        _ => {
            transport::send(
                &mut output,
                1,
                &Reply {
                    session: call.session,
                    result: Ok(1u32),
                },
            )
            .await
            .unwrap();
        }
    }
}
