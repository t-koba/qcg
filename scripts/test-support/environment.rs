/// Run a process-environment test alone with an overall deadline. Call at the
/// start of the test, before constructing tasks or mutating any variables.
pub fn isolated() -> bool {
    let name = std::thread::current().name().expect("test thread must be named").to_owned();
    if std::env::var("QCG_ENV_TEST_CHILD").as_deref() == Ok(&name) { return false; }
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &name, "--nocapture"])
        .env("QCG_ENV_TEST_CHILD", &name).spawn().unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        if let Some(status) = child.try_wait().unwrap() { assert!(status.success(), "isolated test {name} failed"); return true; }
        if std::time::Instant::now() >= deadline { let _ = child.kill(); let _ = child.wait(); panic!("isolated test {name} exceeded 60 seconds"); }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}
