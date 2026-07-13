use std::{env, fs, io::Read, process::ExitCode};

use maker_core::{reconcile, ReconcilePlan, ReconcilePolicy};
use maker_domain::{LiveOrder, OrderIntent};
use maker_risk::{evaluate, BookAge, RiskLimits, RiskReport};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct DryRunInput {
    desired: Vec<OrderIntent>,
    actual: Vec<LiveOrder>,
    books: Vec<BookAge>,
    risk_limits: RiskLimits,
    #[serde(default)]
    reconcile_policy: ReconcilePolicy,
}

#[derive(Debug, Serialize)]
struct DryRunOutput {
    mode: &'static str,
    can_execute: bool,
    risk: RiskReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    plan: Option<ReconcilePlan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

fn main() -> ExitCode {
    match run() {
        Ok(output) => {
            println!("{}", serde_json::to_string_pretty(&output).unwrap());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("maker-dry-run: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<DryRunOutput, Box<dyn std::error::Error>> {
    let input_text = match env::args().nth(1).as_deref() {
        Some(path) if path != "-" => fs::read_to_string(path)?,
        _ => {
            let mut text = String::new();
            std::io::stdin().read_to_string(&mut text)?;
            text
        }
    };
    let input: DryRunInput = serde_json::from_str(&input_text)?;
    let risk = evaluate(&input.desired, &input.books, &input.risk_limits);
    if !risk.allowed {
        return Ok(DryRunOutput {
            mode: "dry_run",
            can_execute: false,
            risk,
            plan: None,
            error: None,
        });
    }

    match reconcile(&input.desired, &input.actual, &input.reconcile_policy) {
        Ok(plan) => Ok(DryRunOutput {
            mode: "dry_run",
            can_execute: true,
            risk,
            plan: Some(plan),
            error: None,
        }),
        Err(error) => Ok(DryRunOutput {
            mode: "dry_run",
            can_execute: false,
            risk,
            plan: None,
            error: Some(error.to_string()),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_is_valid_input() {
        let fixture = include_str!("../../../fixtures/shared_plan.json");
        let input: DryRunInput = serde_json::from_str(fixture).unwrap();
        assert_eq!(input.desired.len(), 2);
        assert_eq!(input.actual.len(), 2);
    }
}
