use camino::Utf8PathBuf;
use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::TcpListener,
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct Fixture(Utf8PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = Utf8PathBuf::from_path_buf(std::env::temp_dir())
            .unwrap()
            .join(format!("install-archive-limit-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_qcg"));
        command
            .current_dir(&self.0)
            .env_remove("PROVIDERS")
            .env("AUTO_GC", "false")
            .env("DATA_HOME", self.0.join("data"));
        for name in [
            "PACKAGE_MAX_ARCHIVE_BYTES",
            "PACKAGE_MAX_METADATA_BYTES",
            "PACKAGE_MAX_ENTRIES",
            "PACKAGE_MAX_BYTES",
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            command.env_remove(name);
        }
        command
    }
    fn install(&self, source: &str, digest: &str) -> Command {
        let mut command = self.command();
        command
            .args([
                "install",
                source,
                "--yes",
                "--sha256",
                digest,
                "--generators-dir",
            ])
            .arg(self.0.join(format!("installed-{}", uuid::Uuid::now_v7())));
        command
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn file_url(path: &Utf8PathBuf) -> String {
    format!("file://{}", path.as_str().replace('\\', "/"))
}

// One bounded loopback response; never contacts a registry or provider.
fn serve(response: Vec<u8>) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}/package.pkg", listener.local_addr().unwrap());
    let task = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "no install request received");
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("{error}"),
            }
        };
        socket
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
            assert!(request.len() < 8192);
        }
        socket.write_all(&response).unwrap();
    });
    (url, task)
}

fn chunked(bytes: &[u8]) -> Vec<u8> {
    let mut response =
        b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_vec();
    for chunk in bytes.chunks(512) {
        write!(response, "{:x}\r\n", chunk.len()).unwrap();
        response.extend_from_slice(chunk);
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"0\r\n\r\n");
    response
}

#[test]
fn install_archive_boundary_and_flag_env_precedence_for_local_and_http() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fixture = Fixture::new();
    let source = fixture.0.join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(
        source.join(contract::MANIFEST_FILE),
        "[generator]\nid = \"archive-limit\"\nname = \"Archive Limit\"\nversion = \"1.0.0\"\n",
    )
    .unwrap();
    let archive = fixture.0.join("package.pkg");
    let output = fixture
        .command()
        .arg("package")
        .arg(&source)
        .arg("--output")
        .arg(&archive)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr(&output));
    let bytes = std::fs::read(&archive).unwrap();
    let digest = hex::encode(Sha256::digest(&bytes));
    let size = bytes.len() as u64;
    for transport in ["path", "file", "http"] {
        // Omitted default, exact/lower flag and env limits, and flag-over-env.
        for (flag, env, success) in [
            (None, None, true),
            (Some(size), None, true),
            (Some(size - 1), None, false),
            (None, Some(size), true),
            (None, Some(size - 1), false),
            (Some(size), Some(size - 1), true),
        ] {
            let (source, server) = match transport {
                "http" => {
                    let (url, task) = serve(chunked(&bytes));
                    (url, Some(task))
                }
                "file" => (file_url(&archive), None),
                _ => (archive.to_string(), None),
            };
            let mut command = fixture.install(&source, &digest);
            if let Some(limit) = flag {
                command.args(["--max-archive-bytes", &limit.to_string()]);
            }
            if let Some(limit) = env {
                command.env("PACKAGE_MAX_ARCHIVE_BYTES", limit.to_string());
            }
            let output = command.output().unwrap();
            if let Some(server) = server {
                server.join().unwrap();
            }
            assert_eq!(
                output.status.success(),
                success,
                "{transport} {flag:?} {env:?}: {}",
                stderr(&output)
            );
            if !success {
                assert!(stderr(&output).contains("exceeds"), "{}", stderr(&output));
            }
        }
    }
}

#[test]
fn default_archive_transport_boundary_and_raised_override() {
    let _serial = TEST_LOCK.lock().unwrap();
    let fixture = Fixture::new();
    let archive = fixture.0.join("oversize.pkg");
    let limit = service::DEFAULT_PACKAGE_MAX_ARCHIVE_BYTES;
    // Deliberately invalid archive: a checksum mismatch proves the read passed
    // the transport gate, without relaxing the independent decompression gates.
    for size in [limit, limit + 1] {
        std::fs::File::create(&archive)
            .unwrap()
            .set_len(size)
            .unwrap();
        for http in [false, true] {
            let (source, server) = if http {
                let mut response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nConnection: close\r\n\r\n"
                )
                .into_bytes();
                // Above the default, advertised length must reject without a body.
                if size == limit {
                    response.resize(response.len() + size as usize, 0);
                }
                let (url, task) = serve(response);
                (url, Some(task))
            } else {
                (archive.to_string(), None)
            };
            let output = fixture.install(&source, &"0".repeat(64)).output().unwrap();
            if let Some(server) = server {
                server.join().unwrap();
            }
            assert!(!output.status.success());
            let expected = if size == limit {
                "SHA-256 mismatch"
            } else {
                "exceeds"
            };
            assert!(stderr(&output).contains(expected), "{}", stderr(&output));
        }
    }
    // Both override surfaces can raise the provisional policy above 16 MiB.
    for flag in [false, true] {
        let mut command = fixture.install(archive.as_str(), &"0".repeat(64));
        if flag {
            command.args(["--max-archive-bytes", &(limit + 1).to_string()]);
        } else {
            command.env("PACKAGE_MAX_ARCHIVE_BYTES", (limit + 1).to_string());
        }
        let output = command.output().unwrap();
        assert!(!output.status.success());
        assert!(
            stderr(&output).contains("SHA-256 mismatch"),
            "{}",
            stderr(&output)
        );
    }
}
