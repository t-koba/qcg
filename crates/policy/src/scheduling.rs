//! Deployment-owned preemption choices; never accepted from run callers.
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

/// FIFO position used only when the scheduler requeues a preempted run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PreemptionQueuePosition {
    #[default]
    Preserve,
    Tail,
}

impl PreemptionQueuePosition {
    /// Strict spellings shared by host/server environment resolution.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "preserve" => Ok(Self::Preserve),
            "tail" => Ok(Self::Tail),
            _ => Err("expected preserve or tail".into()),
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

/// Deployment-owned selection of elapsed wall durations to charge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ElapsedBudgetMode {
    #[default]
    WallClock,
    ActiveExecution,
}

impl ElapsedBudgetMode {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "wall_clock" => Ok(Self::WallClock),
            "active_execution" => Ok(Self::ActiveExecution),
            _ => Err("expected wall_clock or active_execution".into()),
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
    fn elapsed_mode_defaults_and_strict_spellings() {
        assert_eq!(ElapsedBudgetMode::default(), ElapsedBudgetMode::WallClock);
        assert_eq!(
            ElapsedBudgetMode::parse("wall_clock").unwrap(),
            ElapsedBudgetMode::WallClock
        );
        assert_eq!(
            ElapsedBudgetMode::parse("active_execution").unwrap(),
            ElapsedBudgetMode::ActiveExecution
        );
        for bad in [
            "",
            "active",
            "wall",
            "ActiveExecution",
            " wall_clock",
            "active_execution ",
        ] {
            assert!(ElapsedBudgetMode::parse(bad).is_err());
        }
    }

    #[test]
    fn preemption_queue_position_is_strict_and_preserve_by_default() {
        assert_eq!(
            PreemptionQueuePosition::default(),
            PreemptionQueuePosition::Preserve
        );
        assert_eq!(
            PreemptionQueuePosition::parse("preserve").unwrap(),
            PreemptionQueuePosition::Preserve
        );
        assert_eq!(
            PreemptionQueuePosition::parse("tail").unwrap(),
            PreemptionQueuePosition::Tail
        );
        for bad in [
            "",
            "TRUE",
            " tail",
            "tail ",
            "Preserve",
            "step_boundary",
            "false",
        ] {
            assert!(PreemptionQueuePosition::parse(bad).is_err());
        }
    }

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
