//!
//! This file defines the policies that control plane uses to make
//! decisions about rebuild workflow behaviour in case of a child
//! becoming faulted. We can logically provide multiple different policies.
//! These policies are represented as an enum, since only one of the policy
//! can be applied to a given volume/nexus at a time. Once applied, the
//! behaviour of the policy can vary depending on runtime state of the nexus.
//! Any new policy introduced in future will have to implement Policy trait
//! for defining the policy behaviour.

#![allow(unused)]

use stor_port::types::v0::transport::Nexus;

use crate::controller::registry::Registry;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// time constants, in seconds
/// A time period optimized for rebuild performance i.e. preferring log-based rebuild.
const TWAIT_SPERF: std::time::Duration = Duration::new(600, 0);
/// A time period optimized for better redundancy and quicker rebuild decisions.
const TWAIT_SAVAIL: std::time::Duration = Duration::new(300, 0);
const TWAIT_ZERO: std::time::Duration = Duration::new(0, 0);
// 100GiB = 100 * 1024 * 1024 * 1024 bytes
const VOL_SIZE_100GIB: u64 = 107374182400;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
/// SystemPerf policy, optimized for rebuild performance.
pub struct SystemPerf {}
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq)]
/// SystemAvail policy, optimized to maintain replica redundancy.
pub struct SystemAvail {}

/// Set of policies that internally define their rules regarding
/// partial rebuild feasibility.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Copy)]
pub enum RuleSet {
    SystemPerf(SystemPerf),
    SystemAvail(SystemAvail),
}

impl RuleSet {
    /// Returns a duration value. The caller can use this duration
    /// to timeout between two time Instants, particularly to wait
    /// upon a faulted child to possibly be healthy again.
    pub(crate) fn faulted_child_wait(nexus: &Nexus, registry: &Registry) -> Duration {
        let cli_twait = registry.faulted_child_wait_period();
        if !cli_twait.is_zero() {
            return cli_twait;
        }

        let _pol = Self::assign(nexus.size);
        Self::default_faulted_child_timewait()

        // TODO: Enable when policies are dynamic
        /* match pol {
            RuleSet::SystemAvail(pol) => pol.faulted_child_wait_duration(nexus),
            RuleSet::SystemPerf(pol) => pol.faulted_child_wait_duration(nexus),
        } */
    }

    /// Assign a rebuild policy to the nexus.
    fn assign(size: u64) -> Self {
        match size > VOL_SIZE_100GIB {
            true => RuleSet::SystemPerf(SystemPerf {}),
            false => RuleSet::default(),
        }
    }

    fn default_faulted_child_timewait() -> Duration {
        tracing::info!("default timed-wait TWAIT_SPERF(10 mins)");
        TWAIT_SPERF
    }
}

impl Default for RuleSet {
    fn default() -> Self {
        RuleSet::SystemAvail(SystemAvail {})
    }
}

trait Policy {
    fn faulted_child_wait_duration(&self, nexus: &Nexus) -> Duration;
    fn faulted_child_state_allows_wait(&self, nexus: &Nexus) -> bool;
}

impl Policy for SystemPerf {
    fn faulted_child_wait_duration(&self, _nexus: &Nexus) -> Duration {
        TWAIT_SPERF
    }

    fn faulted_child_state_allows_wait(&self, _nexus: &Nexus) -> bool {
        todo!()
    }
}

impl Policy for SystemAvail {
    fn faulted_child_wait_duration(&self, nexus: &Nexus) -> Duration {
        let mut online_child_cnt = 0;
        for _ in nexus.children.iter().filter(|c| c.state.online()) {
            online_child_cnt += 1;
        }
        if online_child_cnt <= 1 {
            // immediate full rebuild
            return TWAIT_ZERO;
        }

        TWAIT_SAVAIL
    }

    fn faulted_child_state_allows_wait(&self, _nexus: &Nexus) -> bool {
        todo!()
    }
}
