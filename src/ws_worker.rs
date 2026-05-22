use crate::dedup::Dedup;
use base64::Engine;
use fastwebsockets::{FragmentCollector, Payload, handshake};
use http::{Request, Uri};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use url::Url;

use hyper_util::rt::TokioExecutor;

#[derive(Clone, Debug)]
pub struct HeartbeatConfig {
    pub text: String,
    pub interval: Duration,
}

pub async fn run_worker(
    worker_id: usize,
    target_url: Url,
    subscription_payload: Option<String>,
    heartbeat: Option<HeartbeatConfig>,
    dedup: Arc<Dedup>,
    output_tx: flume::Sender<Vec<u8>>,
) {
    loop {
        if let Err(e) = connect_and_listen(
            worker_id,
            &target_url,
            subscription_payload.as_deref(),
            heartbeat.as_ref(),
            dedup.clone(),
            output_tx.clone(),
        )
        .await
        {
            eprintln!(
                "[Worker {}] Connection error: {}. Reconnecting in 1s...",
                worker_id, e
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn connect_and_listen(
    _worker_id: usize,
    target_url: &Url,
    subscription_payload: Option<&str>,
    heartbeat: Option<&HeartbeatConfig>,
    dedup: Arc<Dedup>,
    output_tx: flume::Sender<Vec<u8>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let host = target_url.host_str().ok_or("No host in URL")?;
    let port = target_url.port_or_known_default().ok_or("No port in URL")?;
    let addr = format!("{}:{}", host, port);

    let tcp_stream = TcpStream::connect(&addr).await?;
    tcp_stream.set_nodelay(true)?;

    let nonce: [u8; 16] = rand::random();
    let sec_websocket_key = base64::engine::general_purpose::STANDARD.encode(nonce);

    let uri: Uri = target_url.as_str().parse()?;

    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .header("Host", host)
        .header("Upgrade", "websocket")
        .header("Connection", "Upgrade")
        .header("Sec-WebSocket-Key", sec_websocket_key)
        .header("Sec-WebSocket-Version", "13")
        .body(http_body_util::Empty::<hyper::body::Bytes>::new())?;

    // Handle TLS if wss
    let mut ws = if target_url.scheme() == "wss" {
        let mut root_cert_store = rustls::RootCertStore::empty();
        root_cert_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_cert_store)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(config));
        let domain = rustls::pki_types::ServerName::try_from(host.to_string())?;

        let tls_stream = connector.connect(domain, tcp_stream).await?;

        let (ws, _response) = handshake::client(&TokioExecutor::new(), req, tls_stream).await?;
        ws
    } else {
        let (ws, _response) = handshake::client(&TokioExecutor::new(), req, tcp_stream).await?;
        ws
    };

    if let Some(payload) = subscription_payload {
        ws.write_frame(fastwebsockets::Frame::text(Payload::Owned(
            payload.as_bytes().to_vec(),
        )))
        .await?;
    }

    let mut ws = FragmentCollector::new(ws);
    let mut next_heartbeat =
        heartbeat.map(|heartbeat| tokio::time::Instant::now() + heartbeat.interval);

    loop {
        let frame = if let (Some(heartbeat), Some(deadline)) = (heartbeat, next_heartbeat) {
            match tokio::time::timeout_at(deadline, ws.read_frame()).await {
                Ok(frame) => frame?,
                Err(_) => {
                    ws.write_frame(fastwebsockets::Frame::text(Payload::Owned(
                        heartbeat.text.as_bytes().to_vec(),
                    )))
                    .await?;
                    next_heartbeat = Some(tokio::time::Instant::now() + heartbeat.interval);
                    continue;
                }
            }
        } else {
            ws.read_frame().await?
        };

        match frame.opcode {
            fastwebsockets::OpCode::Text | fastwebsockets::OpCode::Binary => {
                let payload = frame.payload;
                if dedup.is_unique(payload.as_ref()) {
                    let _ = output_tx.send(payload.into());
                }
            }
            fastwebsockets::OpCode::Close => {
                break;
            }
            _ => {}
        }
    }

    Ok(())
}
