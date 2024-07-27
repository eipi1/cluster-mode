use crate::common::MpscNode;
use almost_raft::ClusterNode;
use anyhow::anyhow;
use async_trait::async_trait;
use rust_cloud_discovery::{DiscoveryService, ServiceInstance};
use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::sync::{Arc, LockResult, RwLock};

type NodeList = Arc<RwLock<HashMap<String, ServiceInstance>>>;
#[derive(Clone)]
pub struct MpscDiscoveryService {
    nodes: NodeList,
}

impl MpscDiscoveryService {
    pub fn new() -> Self {
        MpscDiscoveryService {
            nodes: Default::default(),
        }
    }

    pub fn register(&self, node_id: String) {
        let instance = ServiceInstance::new(
            Some(node_id.clone()),
            None,
            None,
            None,
            false,
            None,
            HashMap::new(),
            None,
        );
        if let Ok(mut guard) = self.nodes.write() {
            guard.insert(node_id, instance);
        };
    }
}

#[async_trait]
impl DiscoveryService for MpscDiscoveryService {
    async fn discover_instances(&self) -> Result<Vec<ServiceInstance>, Box<dyn Error>> {
        match self.nodes.read() {
            Ok(guard) => Ok(guard.values().cloned().collect::<Vec<ServiceInstance>>()),
            Err(err) => Err(anyhow!("lock poisoned").into()),
        }
    }
}
