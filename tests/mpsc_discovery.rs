mod mpsc_node;

use crate::mpsc_node::MpscNode;
use almost_raft::ClusterNode;
use async_trait::async_trait;
use rust_cloud_discovery::{DiscoveryService, ServiceInstance};
use std::collections::{HashMap, HashSet};
use std::error::Error;

type NodeList = HashMap<String, ServiceInstance>;
pub struct MpscDiscoveryService {
    nodes: NodeList,
}

impl MpscDiscoveryService {
    pub fn register(&mut self, mpsc_node: MpscNode) {
        let id = mpsc_node.node_id.to_string();
        let instance = ServiceInstance::new(
            Some(id.clone()),
            None,
            None,
            None,
            false,
            None,
            HashMap::new(),
            None,
        );
        self.nodes.insert(id, instance);
    }
}

#[async_trait]
impl DiscoveryService for MpscDiscoveryService {
    async fn discover_instances(&self) -> Result<Vec<ServiceInstance>, Box<dyn Error>> {
        let values = self
            .nodes
            .values()
            .cloned()
            .collect::<Vec<ServiceInstance>>();
        Ok(values)
    }
}
