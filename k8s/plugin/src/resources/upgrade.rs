use crate::resources::{
    constant::{
        UPGRADE_CONTROLLER_DEPLOYMENT, UPGRADE_IMAGE, UPGRADE_OPERATOR_CLUSTER_ROLE,
        UPGRADE_OPERATOR_CLUSTER_ROLE_BINDING, UPGRADE_OPERATOR_NAMESPACE,
        UPGRADE_OPERATOR_SERVICE, UPGRADE_OPERATOR_SERVICE_ACCOUNT,
    },
    objects, upgradeoperatorclient, Upgrade,
};
use anyhow::Error;
use async_trait::async_trait;
use k8s_openapi::api::{
    apps::v1::Deployment,
    core::v1::{Service, ServiceAccount},
    rbac::v1::{ClusterRole, ClusterRoleBinding},
};
use kube::{
    api::{Api, DeleteParams, ListParams, PostParams},
    core::ObjectList,
    Client,
};
use std::time::Duration;

use std::path::PathBuf;

pub struct Upgrades {
    pub(crate) upgrade_service_account: Api<ServiceAccount>,
    pub(crate) upgrade_cluster_role: Api<ClusterRole>,
    pub(crate) upgrade_cluster_role_binding: Api<ClusterRoleBinding>,
    pub(crate) upgrade_deployment: Api<Deployment>,
    pub(crate) upgrade_service: Api<Service>,
}

/// Methods implemented by Upgrades
impl Upgrades {
    /// Returns an instance of Upgrades
    pub async fn new() -> anyhow::Result<Self, Error> {
        let client = Client::try_default().await?;
        Ok(Self {
            upgrade_service_account: Api::<ServiceAccount>::namespaced(
                client.clone(),
                UPGRADE_OPERATOR_NAMESPACE,
            ),
            upgrade_cluster_role: Api::<ClusterRole>::all(client.clone()),
            upgrade_cluster_role_binding: Api::<ClusterRoleBinding>::all(client.clone()),
            upgrade_deployment: Api::<Deployment>::namespaced(
                client.clone(),
                UPGRADE_OPERATOR_NAMESPACE,
            ),
            upgrade_service: Api::<Service>::namespaced(client, UPGRADE_OPERATOR_NAMESPACE),
        })
    }

    /// Install the upgrade resources
    pub async fn install() {
        match Upgrades::new().await {
            Ok(upgrade_object) => upgrade_object.install().await,
            Err(e) => println!("Failed to install. Error {}", e),
        };
    }

    /// Apply the upgrade
    pub async fn apply(
        kube_config_path: Option<PathBuf>,
        timeout: humantime::Duration,
        uri: Option<String>,
    ) {
        match Upgrades::new().await {
            Ok(apply_upgrade) => apply_upgrade.apply(kube_config_path, timeout, uri).await,
            Err(e) => println!("Failed to uninstall. Error {}", e),
        };
    }

    /// Get the upgrade status
    pub async fn get(
        kube_config_path: Option<PathBuf>,
        timeout: humantime::Duration,
        uri: Option<String>,
    ) {
        match Upgrades::new().await {
            Ok(get_upgrade) => get_upgrade.get(kube_config_path, timeout, uri).await,
            Err(e) => println!("Failed to uninstall. Error {}", e),
        };
    }

    /// Uninstall the upgrade resources
    pub async fn uninstall() {
        match Upgrades::new().await {
            Ok(delete_upgrade_object) => delete_upgrade_object.uninstall().await,
            Err(e) => println!("Failed to uninstall. Error {}", e),
        };
    }

    pub async fn get_service_account(&self) -> ObjectList<ServiceAccount> {
        let lp = ListParams::default().fields(&format!(
            "metadata.name={}",
            UPGRADE_OPERATOR_SERVICE_ACCOUNT
        ));
        self.upgrade_service_account
            .list(&lp)
            .await
            .expect("failed to list service accounts")
    }

    pub async fn get_cluster_role(&self) -> ObjectList<ClusterRole> {
        let lp = ListParams::default()
            .fields(&format!("metadata.name={}", UPGRADE_OPERATOR_CLUSTER_ROLE));
        self.upgrade_cluster_role
            .list(&lp)
            .await
            .expect("failed to list cluster role")
    }

    pub async fn get_cluster_role_binding(&self) -> ObjectList<ClusterRoleBinding> {
        let lp = ListParams::default().fields(&format!(
            "metadata.name={}",
            UPGRADE_OPERATOR_CLUSTER_ROLE_BINDING
        ));
        self.upgrade_cluster_role_binding
            .list(&lp)
            .await
            .expect("failed to list cluster role binding")
    }

    pub async fn get_deployment(&self) -> ObjectList<Deployment> {
        let lp = ListParams::default()
            .fields(&format!("metadata.name={}", UPGRADE_CONTROLLER_DEPLOYMENT));
        self.upgrade_deployment
            .list(&lp)
            .await
            .expect("failed to list deployment")
    }

    pub async fn get_service(&self) -> ObjectList<Service> {
        let lp =
            ListParams::default().fields(&format!("metadata.name={}", UPGRADE_OPERATOR_SERVICE));
        self.upgrade_service
            .list(&lp)
            .await
            .expect("failed to list service")
    }
}

#[async_trait(?Send)]
impl Upgrade for Upgrades {
    async fn install(&self) {
        let pp = PostParams::default();
        // Create a service account
        let sa = self.get_service_account().await;
        if sa.iter().count() == 0 {
            let ns: Option<String> = Some("mayastor".to_string());
            let service_account = objects::upgrade_operator_service_account(ns);
            let pp = PostParams::default();
            match self
                .upgrade_service_account
                .create(&pp.clone(), &service_account)
                .await
            {
                Ok(_) => {
                    println!("service account created");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in creating service account {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("service account already present! ");
        }

        // Create Cluser role
        let cr = self.get_cluster_role().await;
        if cr.iter().count() == 0 {
            let ns: Option<String> = Some("mayastor".to_string());
            let role = objects::upgrade_operator_cluster_role(ns);
            match self.upgrade_cluster_role.create(&pp.clone(), &role).await {
                Ok(_) => {
                    println!("cluster role created");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in creating cluster role {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("cluster role already present!");
        }

        // Create Cluster role binding
        let crb = self.get_cluster_role_binding().await;
        if crb.iter().count() == 0 {
            let ns: Option<String> = Some("mayastor".to_string());
            let role_binding = objects::upgrade_operator_cluster_role_binding(ns);
            match self
                .upgrade_cluster_role_binding
                .create(&pp.clone(), &role_binding)
                .await
            {
                Ok(_) => {
                    println!("cluster role binding created");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in creating cluster role binding {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("cluster role binding already present!");
        }

        // Create Deployment
        let crb = self.get_deployment().await;
        if crb.iter().count() == 0 {
            let ns: Option<String> = Some("mayastor".to_string());
            let upgrade_deploy =
                objects::upgrade_operator_deployment(ns, UPGRADE_IMAGE.to_string());
            match self.upgrade_deployment.create(&pp, &upgrade_deploy).await {
                Ok(_) => {
                    println!("deployment created");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in creating deployment {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("deployment already present!");
        }

        // Create Service
        let crb = self.get_service().await;
        if crb.iter().count() == 0 {
            let ns: Option<String> = Some("mayastor".to_string());
            let upgrade_service = objects::upgrade_operator_service(ns);
            match self.upgrade_service.create(&pp, &upgrade_service).await {
                Ok(_) => {
                    println!("service created");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in creating service {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("service already present!");
        }
    }

    async fn apply(
        &self,
        kube_config_path: Option<PathBuf>,
        timeout: humantime::Duration,
        uri: Option<String>,
    ) {
        let uoc_m = upgradeoperatorclient::UpgradeOperatorClient::new(
            uri,
            kube_config_path,
            "mayastor".to_string(),
            timeout,
        )
        .await;

        if let Some(mut uoc) = uoc_m {
            if let Err(err) = uoc.apply_upgrade().await {
                println!("Error while  upgrading {:?}", err);
            }
        }
    }
    async fn get(
        &self,
        kube_config_path: Option<PathBuf>,
        timeout: humantime::Duration,
        uri: Option<String>,
    ) {
        let uoc_m = upgradeoperatorclient::UpgradeOperatorClient::new(
            uri,
            kube_config_path,
            "mayastor".to_string(),
            timeout,
        )
        .await;

        if let Some(mut uoc) = uoc_m {
            if let Err(err) = uoc.get_upgrade().await {
                println!("Error while getting upgrade status {:?}", err);
            }
        }
    }

    async fn uninstall(&self) {
        let dp = DeleteParams::default();

        // delete service account
        let svca = self.get_service_account().await;
        if svca.iter().count() == 1 {
            match self
                .upgrade_service_account
                .delete(UPGRADE_OPERATOR_SERVICE_ACCOUNT, &dp)
                .await
            {
                Ok(_) => {
                    println!("service account deleted");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in deleting service account {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("service account does not exist!");
        }

        // delete cluster role
        let cr = self.get_cluster_role().await;
        if cr.iter().count() == 1 {
            match self
                .upgrade_cluster_role
                .delete(UPGRADE_OPERATOR_CLUSTER_ROLE, &dp)
                .await
            {
                Ok(_) => {
                    println!("cluster role deleted");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in deleting cluster role {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("cluster role does not exist!");
        }

        // delete cluster role binding
        let crb = self.get_cluster_role_binding().await;
        if crb.iter().count() == 1 {
            match self
                .upgrade_cluster_role_binding
                .delete(UPGRADE_OPERATOR_CLUSTER_ROLE_BINDING, &dp)
                .await
            {
                Ok(_) => {
                    println!("cluster role binding deleted");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in deleting cluster role binding {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("cluster role binding does not exist!");
        }

        // delete deployment
        let deployment = self.get_deployment().await;
        if deployment.iter().count() == 1 {
            match self
                .upgrade_deployment
                .delete(UPGRADE_CONTROLLER_DEPLOYMENT, &dp)
                .await
            {
                Ok(_) => {
                    println!("deployment deleted");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in deleting deployment {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("deployment does not exist!");
        }

        // delete service
        let svc = self.get_service().await;
        if svc.iter().count() == 1 {
            match self
                .upgrade_service
                .delete(UPGRADE_OPERATOR_SERVICE, &dp.clone())
                .await
            {
                Ok(_) => {
                    println!("service deleted");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                Err(e) => {
                    println!("Failed in deleting service {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    std::process::exit(1);
                }
            }
        } else {
            println!("service does not exist!");
        }
    }
}
