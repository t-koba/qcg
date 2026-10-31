use reqwest::{Client, Url, redirect::Policy};

const MAX_REDIRECTS: usize = 3;
fn redirect_allowed(next: &Url, previous: &[Url]) -> bool {
    previous.len() <= MAX_REDIRECTS
        && previous.first().is_some_and(|initial| {
            (next.host_str(), next.username(), next.password())
                == (initial.host_str(), initial.username(), initial.password())
                && (next.scheme() == "https"
                    || next.scheme() == "http"
                        && !previous.iter().any(|url| url.scheme() == "https"))
        })
}

pub(crate) fn client() -> reqwest::Result<Client> {
    Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .redirect(Policy::custom(|attempt| {
            if redirect_allowed(attempt.url(), attempt.previous()) {
                attempt.follow()
            } else {
                attempt.error("CLI redirect refused")
            }
        }))
        .build()
}

pub(crate) async fn get(url: &str, limit: Option<u64>) -> anyhow::Result<Vec<u8>> {
    let mut response = client()?.get(url).send().await?.error_for_status()?;
    let mut left = limit.unwrap_or(u64::MAX);
    if response.content_length().unwrap_or(0) > left {
        anyhow::bail!("remote body exceeds byte limit");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        left = left
            .checked_sub(chunk.len() as u64)
            .ok_or(anyhow::Error::msg("remote body exceeds byte limit"))?;
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn redirect_policy_preserves_host_and_userinfo_without_https_downgrades() {
        for (initial, allowed, refused) in [
            (
                "http://registry.example/index.toml",
                vec![
                    "http://registry.example/next",
                    "http://registry.example:8080/next",
                    "https://registry.example/next",
                    "https://registry.example:8443/next",
                ],
                vec![
                    "http://other.example/next",
                    "https://other.example/next",
                    "https://user@registry.example/next",
                    "ftp://registry.example/next",
                ],
            ),
            (
                "https://registry.example:8443/index.toml",
                vec![
                    "https://registry.example/next",
                    "https://registry.example:9443/next",
                ],
                vec![
                    "http://registry.example/next",
                    "http://registry.example:8080/next",
                    "https://other.example:8443/next",
                    "https://user@registry.example:8443/next",
                ],
            ),
            (
                "http://user:password@registry.example/index.toml",
                vec!["https://user:password@registry.example:8443/next"],
                vec![
                    "https://registry.example/next",
                    "https://other:password@registry.example/next",
                    "https://user:changed@registry.example/next",
                    "https://user@registry.example/next",
                ],
            ),
        ] {
            let previous = [Url::parse(initial).unwrap()];
            for target in allowed {
                assert!(
                    redirect_allowed(&Url::parse(target).unwrap(), &previous),
                    "{initial} -> {target}"
                );
            }
            for target in refused {
                assert!(
                    !redirect_allowed(&Url::parse(target).unwrap(), &previous),
                    "{initial} -> {target}"
                );
            }
        }
    }

    #[test]
    fn redirect_policy_bounds_the_chain_and_remembers_https_upgrade() {
        let http = Url::parse("http://registry.example/archive").unwrap();
        let https = Url::parse("https://registry.example:8443/archive").unwrap();
        let previous = [http.clone(), https.clone()];
        assert!(redirect_allowed(&https, &previous));
        assert!(!redirect_allowed(&http, &previous));
        assert!(!redirect_allowed(&http, &[https.clone(), http.clone()]));
        assert!(!redirect_allowed(&https, &[]));
        assert!(redirect_allowed(
            &https,
            &vec![https.clone(); MAX_REDIRECTS]
        ));
        assert!(!redirect_allowed(
            &https,
            &vec![https.clone(); MAX_REDIRECTS + 1]
        ));
    }

    // A single bounded loopback response; no external server, TLS or DNS needed.
    async fn serve(response: String) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/index.toml", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(5), async {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(socket.read_u8().await.unwrap());
                    assert!(request.len() < 8192);
                }
                socket.write_all(response.as_bytes()).await.unwrap();
            })
            .await
            .unwrap();
        });
        (url, task)
    }

    #[tokio::test]
    async fn index_limit_rejects_content_length_before_reading_body() {
        // No body: refusal must use advertised size, not an EOF/decode failure.
        let (url, server) =
            serve("HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\n".into()).await;
        let error = get(&url, Some(8)).await.unwrap_err();
        assert!(
            error.to_string().contains("exceeds byte limit"),
            "{error:#}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn index_limit_is_enforced_while_streaming_without_content_length() {
        // Each chunk fits separately; the aggregate must be counted.
        let response = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\n1234\r\n4\r\n5678\r\n1\r\n9\r\n0\r\n\r\n";
        let (url, server) = serve(response.into()).await;
        let error = get(&url, Some(8)).await.unwrap_err();
        assert!(
            error.to_string().contains("exceeds byte limit"),
            "{error:#}"
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn body_limit_accepts_exact_boundary_and_explicit_none() {
        for limit in [Some(8), None] {
            let (url, server) = serve("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\n1234\r\n4\r\n5678\r\n0\r\n\r\n".into()).await;
            assert_eq!(get(&url, limit).await.unwrap(), b"12345678");
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn client_refuses_cross_host_redirect_instead_of_returning_success() {
        let (url, server) = serve("HTTP/1.1 302 Found\r\nLocation: http://localhost:1/other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into()).await;
        let error = client().unwrap().get(&url).send().await.unwrap_err();
        assert!(error.is_redirect(), "{error:#}");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_follows_same_host_redirect_across_ports() {
        let (target, target_server) =
            serve("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".into())
                .await;
        let (url, redirect_server) = serve(format!(
            "HTTP/1.1 302 Found\r\nLocation: {target}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        )).await;
        assert_eq!(get(&url, Some(2)).await.unwrap(), b"ok");
        redirect_server.await.unwrap();
        target_server.await.unwrap();
    }

    #[tokio::test]
    async fn client_follows_same_host_redirects_but_bounds_a_loop() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/start", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(5), async {
            for _ in 0..=MAX_REDIRECTS {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(socket.read_u8().await.unwrap());
                    assert!(request.len() < 8192);
                }
                socket.write_all(b"HTTP/1.1 302 Found\r\nLocation: /start\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            }
        }).await.unwrap();
        });
        let error = client().unwrap().get(&url).send().await.unwrap_err();
        assert!(error.is_redirect(), "{error:#}");
        server.await.unwrap();
    }
}
