//! Deployment-owned preemption choice; never accepted from run callers.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PreemptionMode {
    #[default]
    StepBoundary,
    Interrupt,
}

impl PreemptionMode {
    /// Strict spellings shared by host/server environment resolution.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "step_boundary" => Ok(Self::StepBoundary),
            "interrupt" => Ok(Self::Interrupt),
            _ => Err("expected step_boundary or interrupt".into()),
        }
    }

    pub fn from_env(name: &str) -> Result<Self, String> {
        match std::env::var(name) {
            Ok(value) => Self::parse(&value).map_err(|error| format!("{name}: {error}")),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(_) => Err(format!("{name} must be Unicode")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preemption_mode_is_strict_and_boundary_by_default() {
        assert_eq!(PreemptionMode::default(), PreemptionMode::StepBoundary);
        assert_eq!(
            PreemptionMode::parse("interrupt").unwrap(),
            PreemptionMode::Interrupt
        );
        assert_eq!(
            PreemptionMode::parse("step_boundary").unwrap(),
            PreemptionMode::StepBoundary
        );
        for bad in ["", "TRUE", " interrupt", "tail", "false", "StepBoundary"] {
            assert!(PreemptionMode::parse(bad).is_err());
        }
    }
}
