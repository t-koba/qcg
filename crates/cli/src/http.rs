use reqwest::{Client, Url, redirect::Policy};

const MAX_REDIRECTS: usize = 3;

/// This client carries no credentials or cookies, so a release URL may hand a
/// download off to a CDN: a credential-free HTTPS target on another host is
/// safe to follow. Only the same origin keeps the initial userinfo, and no hop
/// may downgrade from HTTPS or revisit a URL.
fn redirect_allowed(next: &Url, previous: &[Url]) -> bool {
    let Some(initial) = previous.first() else {
        return false;
    };
    if previous.len() > MAX_REDIRECTS
        || previous.contains(next)
        || !matches!(next.scheme(), "http" | "https")
        || (next.scheme() == "http" && previous.iter().any(|url| url.scheme() == "https"))
    {
        return false;
    }
    if next.origin() == initial.origin() {
        (next.username(), next.password()) == (initial.username(), initial.password())
    } else {
        next.scheme() == "https" && next.username().is_empty() && next.password().is_none()
    }
}

pub(crate) fn download_client() -> reqwest::Result<Client> {
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

/// Trace export posts a whole trace body; a collector must not be able to
/// replay it to a different origin. Refuse every redirect with the same fixed
/// redacted error used by the download policy.
pub(crate) fn export_client() -> reqwest::Result<Client> {
    Client::builder()
        .timeout(std::time::Duration::from_secs(120))
        .redirect(Policy::custom(|attempt| {
            attempt.error("CLI redirect refused")
        }))
        .build()
}

pub(crate) async fn get(url: &str, limit: Option<u64>) -> anyhow::Result<Vec<u8>> {
    let mut response = download_client()?
        .get(url)
        .send()
        .await?
        .error_for_status()?;
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
    use std::error::Error as _;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn redirect_policy_allows_cross_origin_https_without_credentials() {
        let previous = [Url::parse("https://releases.example/archive.tar.gz").unwrap()];
        for target in [
            "https://cdn.example/archive.tar.gz",
            "https://cdn.example:9443/archive.tar.gz",
            "https://other.example/path?token=x",
        ] {
            assert!(
                redirect_allowed(&Url::parse(target).unwrap(), &previous),
                "{target}"
            );
        }
        for target in [
            "http://cdn.example/archive.tar.gz",
            "https://user@cdn.example/archive.tar.gz",
            "https://user:password@cdn.example/archive.tar.gz",
            "ftp://cdn.example/archive.tar.gz",
            "file:///etc/passwd",
        ] {
            assert!(
                !redirect_allowed(&Url::parse(target).unwrap(), &previous),
                "{target}"
            );
        }
    }

    #[test]
    fn redirect_policy_refuses_https_downgrade_on_every_hop() {
        let http = Url::parse("http://releases.example/archive").unwrap();
        let https = Url::parse("https://releases.example/archive").unwrap();
        let cdn_http = Url::parse("http://cdn.example/archive").unwrap();
        for target in [&http, &cdn_http] {
            assert!(!redirect_allowed(target, std::slice::from_ref(&https)));
            assert!(!redirect_allowed(target, &[http.clone(), https.clone()]));
        }
        // A port change is also a new origin and cannot carry userinfo;
        // a credential-free HTTPS upgrade remains allowed.
        assert!(!redirect_allowed(
            &Url::parse("http://releases.example:8080/archive").unwrap(),
            std::slice::from_ref(&http)
        ));
        assert!(redirect_allowed(&https, &[http]));
    }

    #[test]
    fn redirect_policy_refuses_cross_origin_http_and_allows_https_upgrade() {
        let http = Url::parse("http://127.0.0.1:8080/archive").unwrap();
        for target in [
            "http://127.0.0.2:8080/archive",
            "http://other.example/archive",
            "ftp://other.example/archive",
        ] {
            assert!(
                !redirect_allowed(&Url::parse(target).unwrap(), std::slice::from_ref(&http)),
                "{target}"
            );
        }
        // An upgrade to HTTPS is allowed even when the host changes.
        assert!(redirect_allowed(
            &Url::parse("https://cdn.example/archive").unwrap(),
            &[http]
        ));
    }

    #[test]
    fn redirect_policy_keeps_same_host_userinfo_and_refuses_foreign_userinfo() {
        let initial = Url::parse("http://user:password@releases.example/archive").unwrap();
        assert!(redirect_allowed(
            &Url::parse("http://user:password@releases.example/next").unwrap(),
            std::slice::from_ref(&initial)
        ));
        for cross_origin in [
            "https://user:password@releases.example/archive",
            "http://user:password@releases.example/archive",
            "http://user:password@releases.example:8443/archive",
        ] {
            assert!(
                !redirect_allowed(
                    &Url::parse(cross_origin).unwrap(),
                    std::slice::from_ref(&initial)
                ),
                "a scheme or port change is a new origin: {cross_origin}"
            );
        }
        for target in [
            "https://other:password@releases.example/archive",
            "https://user:changed@releases.example/archive",
            "https://user@releases.example/archive",
            "https://user:password@cdn.example/archive",
        ] {
            assert!(
                !redirect_allowed(&Url::parse(target).unwrap(), std::slice::from_ref(&initial)),
                "{target}"
            );
        }
        // Dropping userinfo across origins forwards nothing to the target.
        assert!(redirect_allowed(
            &Url::parse("https://cdn.example/archive").unwrap(),
            std::slice::from_ref(&initial)
        ));
        assert!(redirect_allowed(
            &Url::parse("https://releases.example/archive").unwrap(),
            std::slice::from_ref(&initial)
        ));
    }

    #[test]
    fn redirect_policy_bounds_hops_and_refuses_visited_urls() {
        let chain: Vec<Url> = (0..MAX_REDIRECTS)
            .map(|hop| Url::parse(&format!("https://releases.example/{hop}")).unwrap())
            .collect();
        let next = Url::parse("https://releases.example/final").unwrap();
        assert!(redirect_allowed(&next, &chain));
        let mut over_budget = chain;
        over_budget.push(next.clone());
        assert!(!redirect_allowed(&next, &over_budget));

        // A loop is refused well inside the hop budget.
        let first = Url::parse("https://releases.example/archive").unwrap();
        let cdn = Url::parse("https://cdn.example/archive").unwrap();
        assert!(!redirect_allowed(&first, &[first.clone(), cdn.clone()]));
        assert!(!redirect_allowed(&cdn, &[first.clone(), cdn.clone()]));
        assert!(!redirect_allowed(&first, &[]));
    }

    // reqwest prefixes its own `Display` with the visited URL; the message
    // supplied by the policy is the fixed, URL-free source error.
    fn assert_fixed_redirect_error(error: &reqwest::Error) {
        assert!(error.is_redirect(), "{error:#}");
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("CLI redirect refused"),
            "{error:#}"
        );
    }

    // Drain the full request before answering. If a loopback server drops a
    // socket while request bytes are still unread (a POST body, or the whole
    // request for a server that never reads), Windows TCP answers with RST
    // (os error 10053), which races the response and turns a redirect
    // assertion into a connection abort. Linux/macOS discard the unread bytes
    // and send FIN, so the flake only showed up on windows-latest.
    async fn read_request(socket: &mut tokio::net::TcpStream) {
        let mut head = Vec::new();
        while !head.ends_with(b"\r\n\r\n") {
            head.push(socket.read_u8().await.unwrap());
            assert!(head.len() < 8192, "request head must be bounded");
        }
        let content_length = String::from_utf8_lossy(&head)
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap_or(0);
        assert!(content_length <= 8192, "request body must be bounded");
        let mut body = vec![0_u8; content_length];
        socket.read_exact(&mut body).await.unwrap();
    }

    // A single bounded loopback response; no external server, TLS or DNS needed.
    async fn serve(response: String) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/index.toml", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(5), async {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
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
        let error = download_client()
            .unwrap()
            .get(&url)
            .send()
            .await
            .unwrap_err();
        assert_fixed_redirect_error(&error);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn export_client_refuses_all_redirects() {
        let (url, server) = serve("HTTP/1.1 302 Found\r\nLocation: https://other.example/trace\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into()).await;
        let error = export_client()
            .unwrap()
            .post(&url)
            .body("trace")
            .send()
            .await
            .unwrap_err();
        assert_fixed_redirect_error(&error);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_follows_a_same_origin_redirect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/start", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            for response in [
                "HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                read_request(&mut socket).await;
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        assert_eq!(get(&url, Some(2)).await.unwrap(), b"ok");
        task.await.unwrap();
    }

    #[tokio::test]
    async fn client_refuses_a_redirect_to_a_visited_url() {
        let (url, server) = serve(
            "HTTP/1.1 302 Found\r\nLocation: /index.toml\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                .into(),
        )
        .await;
        let error = download_client()
            .unwrap()
            .get(&url)
            .send()
            .await
            .unwrap_err();
        assert_fixed_redirect_error(&error);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn client_bounds_distinct_same_host_redirects() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/hop-0", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(5), async {
                for hop in 0..=MAX_REDIRECTS {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    read_request(&mut socket).await;
                    let next = hop + 1;
                    socket
                        .write_all(
                            format!("HTTP/1.1 302 Found\r\nLocation: /hop-{next}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                                .as_bytes(),
                        )
                        .await
                        .unwrap();
                }
            })
            .await
            .unwrap();
        });
        let error = download_client()
            .unwrap()
            .get(&url)
            .send()
            .await
            .unwrap_err();
        assert_fixed_redirect_error(&error);
        server.await.unwrap();
    }
}
