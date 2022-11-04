use async_trait::async_trait;
use rust_cloud_discovery::{DiscoveryService, ServiceInstance};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;

// mod node;

pub struct TestDiscoveryService {
    instances: Arc<RwLock<Vec<ServiceInstance>>>,
}

impl TestDiscoveryService {
    pub fn new(instances: Arc<RwLock<Vec<ServiceInstance>>>) -> Self {
        Self{
            instances
        }
    }
}

#[async_trait]
impl DiscoveryService for TestDiscoveryService {
    async fn discover_instances(&self) -> Result<Vec<ServiceInstance>, Box<dyn Error>> {
        // let mut rx = self.rx.write().await;
        // let result = tokio::time::timeout(
        //     Duration::from_millis(remaining_update_interval),
        //     rx.recv()
        // ).await;
        // if
        let guard = self.instances.read().await;
        Ok(guard.to_vec())
    }
}
