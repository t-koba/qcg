mod bounded_http;
mod bounded_stdio;
mod mcp;

pub use mcp::*;
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use url::Url;

    fn remote_spec() -> McpServerSpec {
        McpServerSpec {
            id: "tinyfish".into(),
            transport: McpTransport::StreamableHttp,
            lifecycle: McpLifecycle::Initialize,
            url: Some("https://agent.tinyfish.ai/mcp".into()),
            command: vec![],
            env: BTreeMap::new(),
            env_from: BTreeMap::new(),
            headers: BTreeMap::new(),
            auth: McpAuth::Oauth,
            credential_env: None,
            auth_header: None,
            auth_prefix: String::new(),
            oauth_scopes: vec![],
            oauth_client_id_env: None,
            oauth_client_secret_env: None,
            oauth_store: OAuthCredentialStore::Memory,
            allowed_hosts: vec!["agent.tinyfish.ai".into()],
            timeout_seconds: 30,
            max_response_bytes: 1024,
        }
    }

    #[test]
    fn transport_limits_have_no_hard_ceiling() {
        let mut timeout = remote_spec();
        timeout.timeout_seconds = u64::MAX;
        timeout
            .validate()
            .expect("explicit large timeout must validate");

        let mut response = remote_spec();
        response.max_response_bytes = usize::MAX;
        response
            .validate()
            .expect("explicit large response limit must validate");
    }

    #[test]
    fn public_defaults_are_anonymous_and_pinned_to_exact_hosts() {
        let runtime = McpRuntime::public_defaults();
        assert_eq!(runtime.server_ids(), ["exa-public", "parallel-public"]);
        for (id, url, host) in [
            ("exa-public", "https://mcp.exa.ai/mcp", "mcp.exa.ai"),
            (
                "parallel-public",
                "https://search.parallel.ai/mcp",
                "search.parallel.ai",
            ),
        ] {
            let profile = runtime.resolve(id).expect("public profile should resolve");
            assert_eq!(profile.spec.auth, McpAuth::None);
            assert_eq!(profile.spec.lifecycle, McpLifecycle::Initialize);
            assert_eq!(profile.spec.url.as_deref(), Some(url));
            assert_eq!(profile.spec.allowed_hosts, [host]);
            assert!(profile.spec.credential_env.is_none());
        }
    }

    #[test]
    fn public_default_ids_cannot_be_overridden() {
        let mut spec = remote_spec();
        spec.id = "exa-public".into();
        let error = McpRuntime::from_specs_with_public_defaults(vec![spec])
            .expect_err("built-in public profile ids must be reserved");
        assert!(error.contains("reserved"), "{error}");
    }

    #[test]
    fn remote_profile_requires_all_endpoint_hosts() {
        let mut spec = remote_spec();
        spec.allowed_hosts.clear();
        let error = spec.validate().expect_err("endpoint host must be allowed");
        assert!(error.contains("allowed_hosts"));
    }

    #[test]
    fn remote_profile_rejects_embedded_credentials() {
        let mut spec = remote_spec();
        spec.url = Some("https://secret@agent.tinyfish.ai/mcp".into());
        assert!(spec.validate().is_err());
    }

    #[test]
    fn profile_debug_and_static_fields_do_not_expose_credentials() {
        let mut spec = remote_spec();
        spec.auth = McpAuth::Header;
        spec.credential_env = Some("QCG_MCP_TOKEN".into());
        spec.auth_header = Some("X-Access-Token".into());
        spec.auth_prefix = "secret-prefix ".into();
        spec.oauth_store = OAuthCredentialStore::Keyring;
        let debug = format!("{spec:?}");
        assert!(!debug.contains("secret-prefix"));
        assert!(debug.contains("<redacted>"));

        let mut static_secret = remote_spec();
        static_secret.auth = McpAuth::None;
        static_secret
            .headers
            .insert("X-API-Key".into(), "secret".into());
        let error = static_secret
            .validate()
            .expect_err("credential-like static headers must be rejected");
        assert!(error.contains("must not contain credentials"));
    }

    #[test]
    fn profile_rejects_transport_headers_and_process_control_environment() {
        for name in [
            "Accept",
            "Authorization",
            "Content-Type",
            "Mcp-Session-Id",
            "Last-Event-Id",
        ] {
            let mut spec = remote_spec();
            spec.headers.insert(name.into(), "configured".into());
            let error = spec
                .validate()
                .expect_err("transport-owned headers must be rejected");
            assert!(error.contains("must not override"), "{name}: {error}");
        }

        let mut spec = remote_spec();
        spec.auth = McpAuth::None;
        spec.headers
            .insert("X-Service-Credential".into(), "configured".into());
        assert!(spec.validate().is_err());

        let mut spec = stdio_spec();
        spec.env
            .insert("NODE_OPTIONS".into(), "--require payload".into());
        assert!(spec.validate().is_err());

        let mut spec = stdio_spec();
        spec.env_from
            .insert("LD_PRELOAD".into(), "QCG_LIBRARY_PATH".into());
        assert!(spec.validate().is_err());
    }

    #[cfg(not(feature = "mcp-oauth"))]
    #[test]
    fn oauth_specs_are_rejected_without_the_oauth_feature() {
        let error = McpRuntime::from_specs(vec![remote_spec()])
            .expect_err("OAuth profiles require the mcp-oauth feature");
        assert!(error.contains("mcp-oauth"), "{error}");
    }

    #[cfg(feature = "mcp-oauth")]
    #[tokio::test]
    async fn authorization_status_does_not_perform_network_discovery() {
        let mut spec = remote_spec();
        spec.url = Some("https://127.0.0.1:1/mcp".into());
        spec.allowed_hosts = vec!["127.0.0.1".into()];
        let runtime = McpRuntime::from_specs(vec![spec]).expect("runtime should load");
        let authorized = tokio::time::timeout(
            Duration::from_millis(100),
            runtime.is_authorized("tinyfish"),
        )
        .await
        .expect("status lookup must remain local")
        .expect("status lookup should succeed");
        assert!(!authorized);
    }

    #[cfg(feature = "mcp-oauth")]
    #[test]
    fn oauth_redirect_requires_https_or_loopback_http() {
        validate_redirect_uri("http://127.0.0.1:43123/callback")
            .expect("loopback redirect should be valid");
        assert!(validate_redirect_uri("http://example.com/callback").is_err());
        assert!(validate_redirect_uri("https://user@example.com/callback").is_err());
        assert!(is_secure_remote_url(
            &Url::parse("https://oauth.example.test/token").expect("valid URL")
        ));
        assert!(is_secure_remote_url(
            &Url::parse("http://localhost:43123/token").expect("valid URL")
        ));
        assert!(!is_secure_remote_url(
            &Url::parse("http://oauth.example.test/token").expect("valid URL")
        ));
    }

    #[test]
    fn reflected_credentials_are_rejected_without_echoing_them() {
        let credential = "mcp-test-secret-value".to_string();
        let error = reject_credential_reflection(
            &serde_json::json!({ "content": credential }),
            &["mcp-test-secret-value".into()],
        )
        .expect_err("credential reflection must fail closed");
        let message = error.to_string();
        assert!(message.contains("reflected credential material"));
        assert!(!message.contains("mcp-test-secret-value"));
    }

    #[test]
    fn input_required_reflection_is_rejected_without_echo() {
        let input = McpInputRequired {
            input_requests: BTreeMap::from([(
                "q1".into(),
                serde_json::json!({
                    "method": "elicitation/create",
                    "params": { "message": "token mcp-test-secret-value" },
                }),
            )]),
            request_state: Some("state-mcp-test-secret-value".into()),
        };
        let error = reject_credential_reflection(&input, &["mcp-test-secret-value".into()])
            .expect_err("InputRequired reflection must fail closed");
        let message = error.to_string();
        assert!(message.contains("reflected credential material"));
        assert!(!message.contains("mcp-test-secret-value"));
    }

    #[cfg(feature = "mcp-oauth")]
    #[tokio::test]
    async fn authorization_cannot_be_cleared_while_sessions_are_active() {
        let runtime = McpRuntime::from_specs(vec![remote_spec()]).expect("runtime should load");
        runtime
            .active_sessions("tinyfish")
            .expect("test registry has the counter")
            .store(1, Ordering::Release);
        let error = runtime
            .clear_authorization("tinyfish")
            .await
            .expect_err("active session must block credential removal");
        assert!(error.to_string().contains("sessions are active"));
    }

    #[test]
    fn stdio_profile_requires_explicit_command_permission() {
        let spec = stdio_spec();
        spec.validate().expect("stdio profile should be valid");
        let runtime = McpRuntime::from_specs(vec![spec]).expect("runtime should load");
        let profile = runtime.resolve("local").expect("profile should resolve");
        let access = McpAccess {
            network_hosts: BTreeSet::new(),
            commands: vec![McpCommandAccess::trusted_host(vec!["other".into()])],
            workspace: std::env::temp_dir(),
        };
        assert!(access.validate(profile).is_err());
    }

    fn stdio_spec() -> McpServerSpec {
        McpServerSpec {
            id: "local".into(),
            transport: McpTransport::Stdio,
            lifecycle: McpLifecycle::Initialize,
            url: None,
            command: vec!["demo-server".into(), "--stdio".into()],
            env: BTreeMap::new(),
            env_from: BTreeMap::new(),
            headers: BTreeMap::new(),
            auth: McpAuth::None,
            credential_env: None,
            auth_header: None,
            auth_prefix: String::new(),
            oauth_scopes: vec![],
            oauth_client_id_env: None,
            oauth_client_secret_env: None,
            oauth_store: OAuthCredentialStore::Memory,
            allowed_hosts: vec![],
            timeout_seconds: 30,
            max_response_bytes: 1024,
        }
    }
}
