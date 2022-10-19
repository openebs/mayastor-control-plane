use crate::{
    resources::constant::{
        APP, LABEL, UPGRADE_CONTROLLER_DEPLOYMENT, UPGRADE_OPERATOR, UPGRADE_OPERATOR_CLUSTER_ROLE,
        UPGRADE_OPERATOR_CLUSTER_ROLE_BINDING, UPGRADE_OPERATOR_HTTP_PORT,
        UPGRADE_OPERATOR_INTERNAL_PORT, UPGRADE_OPERATOR_SERVICE, UPGRADE_OPERATOR_SERVICE_ACCOUNT,
        UPGRADE_OPERATOR_SERVICE_PORT,
    },
    upgrade_labels,
};
use async_trait::async_trait;
use std::path::PathBuf;

use k8s_openapi::{
    api::{
        apps::v1::{Deployment, DeploymentSpec, DeploymentStrategy},
        core::v1::{
            Container, ContainerPort, EnvVar, PodSpec, PodTemplateSpec, Service, ServiceAccount,
            ServicePort, ServiceSpec,
        },
        rbac::v1::{ClusterRole, ClusterRoleBinding, PolicyRule, RoleRef, Subject},
    },
    apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
};

use kube::core::ObjectMeta;
use maplit::btreemap;

/// Defines the upgrade-operator service account.
pub(crate) fn upgrade_operator_service_account(namespace: Option<String>) -> ServiceAccount {
    ServiceAccount {
        metadata: ObjectMeta {
            labels: Some(upgrade_labels!(UPGRADE_OPERATOR)),
            name: Some(UPGRADE_OPERATOR_SERVICE_ACCOUNT.to_string()),
            namespace,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Defines the upgrade-operator cluster role.
pub(crate) fn upgrade_operator_cluster_role(namespace: Option<String>) -> ClusterRole {
    ClusterRole {
        metadata: ObjectMeta {
            labels: Some(upgrade_labels!(UPGRADE_OPERATOR)),
            name: Some(UPGRADE_OPERATOR_CLUSTER_ROLE.to_string()),
            namespace,
            ..Default::default()
        },
        rules: Some(vec![
            PolicyRule {
                api_groups: Some(vec!["apiextensions.k8s.io".to_string()]),
                resources: Some(vec!["customresourcedefinitions".to_string()]),
                verbs: vec!["create", "list"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["openebs.io".to_string()]),
                resources: Some(vec!["upgradeactions".to_string()]),
                verbs: vec![
                    "get", "create", "list", "watch", "update", "replace", "patch",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["openebs.io".to_string()]),
                resources: Some(vec!["upgradeactions/status".to_string()]),
                verbs: vec!["update", "patch"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["apps".to_string()]),
                resources: Some(vec!["deployments".to_string()]),
                verbs: vec![
                    "create",
                    "delete",
                    "deletecollection",
                    "get",
                    "list",
                    "patch",
                    "update",
                ]
                .iter()
                .map(|s| s.to_string())
                .collect(),
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["pods".to_string()]),
                verbs: vec!["get", "list", "watch", "delete"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["nodes".to_string()]),
                verbs: vec!["get", "list", "patch"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect(),
                ..Default::default()
            },
            PolicyRule {
                api_groups: Some(vec!["".to_string()]),
                resources: Some(vec!["secrets".to_string()]),
                verbs: vec!["get", "list"].iter().map(|s| s.to_string()).collect(),
                ..Default::default()
            },
        ]),
        ..Default::default()
    }
}

/// Defines the upgrade-operator cluster role binding.
pub(crate) fn upgrade_operator_cluster_role_binding(
    namespace: Option<String>,
) -> ClusterRoleBinding {
    ClusterRoleBinding {
        metadata: ObjectMeta {
            labels: Some(upgrade_labels!(UPGRADE_OPERATOR)),
            name: Some(UPGRADE_OPERATOR_CLUSTER_ROLE_BINDING.to_string()),
            namespace: namespace.clone(),
            ..Default::default()
        },
        role_ref: RoleRef {
            api_group: "rbac.authorization.k8s.io".to_string(),
            kind: "ClusterRole".to_string(),
            name: UPGRADE_OPERATOR_CLUSTER_ROLE.to_string(),
        },
        subjects: Some(vec![Subject {
            kind: "ServiceAccount".to_string(),
            name: UPGRADE_OPERATOR_SERVICE_ACCOUNT.to_string(),
            namespace,
            ..Default::default()
        }]),
    }
}

/// Defines the upgrade-operator deployment.
pub(crate) fn upgrade_operator_deployment(
    namespace: Option<String>,
    upgrade_image: String,
) -> Deployment {
    Deployment {
        metadata: ObjectMeta {
            labels: Some(upgrade_labels!(UPGRADE_OPERATOR)),
            name: Some(UPGRADE_CONTROLLER_DEPLOYMENT.to_string()),
            namespace: namespace.clone(),
            ..Default::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(1),
            selector: LabelSelector {
                match_labels: Some(btreemap! { LABEL.to_string() => UPGRADE_OPERATOR.to_string()}),
                ..Default::default()
            },
            strategy: Some(DeploymentStrategy {
                type_: Some("Recreate".to_string()),
                ..Default::default()
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(btreemap! { LABEL.to_string() => UPGRADE_OPERATOR.to_string()}),
                    namespace,
                    ..Default::default()
                }),
                spec: Some(PodSpec {
                    containers: vec![Container {
                        image: Some(upgrade_image),
                        image_pull_policy: Some("Always".to_string()),
                        name: UPGRADE_OPERATOR.to_string(),
                        command: Some(vec!["operator-upgrade".to_string()]),
                        ports: Some(vec![ContainerPort {
                            container_port: 8080,
                            name: Some("http".to_string()),
                            ..Default::default()
                        }]),
                        env: Some(vec![EnvVar {
                            name: "RUST_LOG".to_string(),
                            value: Some("info".to_string()),
                            ..Default::default()
                        }]),
                        // command: Some(vec!["/bin/sh".to_string(), "-c".to_string(),
                        // "operator-upgrade".to_string()]),
                        ..Default::default()
                    }],
                    service_account_name: Some(UPGRADE_OPERATOR_SERVICE_ACCOUNT.to_string()),
                    ..Default::default()
                }),
            },
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Defines the upgrade-operator service.
pub(crate) fn upgrade_operator_service(namespace: Option<String>) -> Service {
    Service {
        metadata: ObjectMeta {
            labels: Some(upgrade_labels!(UPGRADE_OPERATOR)),
            name: Some(UPGRADE_OPERATOR_SERVICE.to_string()),
            namespace,
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            selector: Some(btreemap! {
                LABEL.to_string() => UPGRADE_OPERATOR.to_string()
            }),
            ports: Some(vec![ServicePort {
                port: UPGRADE_OPERATOR_SERVICE_PORT,
                name: Some(UPGRADE_OPERATOR_HTTP_PORT.to_string()),
                // node_port: Some(UPGRADE_OPERATOR_NODE_PORT),
                target_port: Some(IntOrString::Int(UPGRADE_OPERATOR_INTERNAL_PORT)),
                ..Default::default()
            }]),
            // type_ : Some("NodePort".to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// The types of resources that support the 'upgrade' operation.
#[derive(clap::Subcommand, Debug)]
pub enum GetUpgradeResources {
    /// Intall the upgrade operator.
    Install,
    /// Apply the upgrade.
    Apply,
    /// Get the upgrade status.
    Get,
    /// Delete the upgrade operator.
    Uninstall,
}

/// Upgrade trait.
/// To be implemented by resources which support the 'upgrade' operation.
#[async_trait(?Send)]
pub trait Upgrade {
    async fn install(&self);
    async fn apply(
        &self,
        kube_config: Option<PathBuf>,
        timeout: humantime::Duration,
        uri: Option<String>,
    );
    async fn get(
        &self,
        kube_config: Option<PathBuf>,
        timeout: humantime::Duration,
        uri: Option<String>,
    );
    async fn uninstall(&self);
}
