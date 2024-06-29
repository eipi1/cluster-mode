#![allow(unused_variables)]
#![allow(unused_imports)]
use async_trait::async_trait;
use log::trace;
use rust_cloud_discovery::{DiscoveryService, ServiceInstance};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;

// mod node;

#[derive(Debug, Clone)]
pub struct SimpleVecDiscoveryService {
    instances: Arc<RwLock<Vec<ServiceInstance>>>,
}

impl SimpleVecDiscoveryService {
    pub fn new() -> Self {
        Self {
            instances: Arc::new(Default::default()),
        }
    }

    pub async fn add_node(&mut self, instance: ServiceInstance) {
        self.instances.write().await.push(instance);
        trace!("adding new node");
    }

    pub async fn remove_node(&mut self, index: usize) {
        self.instances.write().await.remove(index);
    }
}

#[async_trait]
impl DiscoveryService for SimpleVecDiscoveryService {
    async fn discover_instances(&self) -> Result<Vec<ServiceInstance>, Box<dyn Error>> {
        // let mut rx = self.rx.write().await;
        // let result = tokio::time::timeout(
        //     Duration::from_millis(remaining_update_interval),
        //     rx.recv()
        // ).await;
        // if
        let guard = self.instances.read().await;
        let instances = guard.to_vec();
        trace!("discovered {} nodes", instances.len());
        Ok(instances)
    }
}
