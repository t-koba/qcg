//! Host daemon operation budgets, resolved once by deployment startup.
//! These are not workload limits and never belong in generator contracts.
use std::{ffi::OsString, time::Duration};

/// Frozen daemon policy. Construction validates every value before use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContainerTimeouts {
    create: Duration,
    start: Duration,
    admin: Duration,
    stop: Duration,
}

const DEFAULT_SECONDS: [u64; 4] = [600, 120, 60, 30];

impl Default for ContainerTimeouts {
    fn default() -> Self {
        Self::from_seconds(DEFAULT_SECONDS).expect("valid daemon defaults")
    }
}

impl ContainerTimeouts {
    pub const ENV_NAMES: [&str; 4] = [
        "CONTAINER_CREATE_TIMEOUT_SECS",
        "CONTAINER_START_TIMEOUT_SECS",
        "CONTAINER_ADMIN_TIMEOUT_SECS",
        "CONTAINER_STOP_TIMEOUT_SECS",
    ];

    /// Each operation accepts 1..=3600 seconds. Unset selects its default.
    pub fn from_env() -> Result<Self, String> {
        Self::resolve(|name| std::env::var_os(name))
    }

    /// Pure lookup seam for startup validation and tests; no later env reads.
    pub fn resolve(mut lookup: impl FnMut(&str) -> Option<OsString>) -> Result<Self, String> {
        let mut seconds = DEFAULT_SECONDS;
        for (index, name) in Self::ENV_NAMES.iter().enumerate() {
            if let Some(raw) = lookup(name) {
                seconds[index] = raw
                    .to_str()
                    .filter(|value| {
                        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
                    })
                    .and_then(|value| value.parse().ok())
                    .ok_or_else(|| invalid(name))?;
            }
        }
        Self::from_seconds(seconds)
    }

    pub fn from_seconds(seconds: [u64; 4]) -> Result<Self, String> {
        for (name, value) in Self::ENV_NAMES.iter().zip(seconds) {
            if !(1..=3600).contains(&value) {
                return Err(invalid(name));
            }
        }
        let [create, start, admin, stop] = seconds.map(Duration::from_secs);
        Ok(Self {
            create,
            start,
            admin,
            stop,
        })
    }

    pub fn create(self) -> Duration {
        self.create
    }
    pub fn start(self) -> Duration {
        self.start
    }
    pub fn admin(self) -> Duration {
        self.admin
    }
    pub fn stop(self) -> Duration {
        self.stop
    }
}

fn invalid(name: &str) -> String {
    format!("{name} must be an integer in 1..=3600 seconds")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_overrides_and_freeze() {
        let defaults = ContainerTimeouts::resolve(|_| None).unwrap();
        assert_eq!(
            [
                defaults.create(),
                defaults.start(),
                defaults.admin(),
                defaults.stop()
            ],
            [600, 120, 60, 30].map(Duration::from_secs)
        );
        let mut source = Some(OsString::from("1"));
        let frozen = ContainerTimeouts::resolve(|_| source.clone()).unwrap();
        source = Some(OsString::from("3600"));
        let next = ContainerTimeouts::resolve(|_| source.clone()).unwrap();
        assert_eq!(frozen, ContainerTimeouts::from_seconds([1; 4]).unwrap());
        assert_eq!(next, ContainerTimeouts::from_seconds([3600; 4]).unwrap());
        for (index, name) in ContainerTimeouts::ENV_NAMES.iter().enumerate() {
            let one = ContainerTimeouts::resolve(|key| (key == *name).then(|| "2".into())).unwrap();
            let mut expected = DEFAULT_SECONDS;
            expected[index] = 2;
            assert_eq!(one, ContainerTimeouts::from_seconds(expected).unwrap());
        }
    }

    #[test]
    fn rejects_malformed_zero_overflow_and_range_for_every_variable() {
        for name in ContainerTimeouts::ENV_NAMES {
            for value in [
                "",
                "0",
                "3601",
                "18446744073709551616",
                "-1",
                "+1",
                "1.0",
                " 1",
                "1 ",
                "no",
            ] {
                let error = ContainerTimeouts::resolve(|key| (key == name).then(|| value.into()))
                    .unwrap_err();
                assert!(error.contains(name), "{error}");
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn rejects_non_unicode() {
        use std::os::unix::ffi::OsStringExt;
        assert!(ContainerTimeouts::resolve(|_| Some(OsString::from_vec(vec![255]))).is_err());
    }
}
