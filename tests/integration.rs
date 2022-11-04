use crate::node::TestNode;
use cluster_mode::{start_cluster, Cluster, ClusterConfig, ClusterInfo};
use lazy_static::lazy_static;
use rust_cloud_discovery::{DiscoveryClient, ServiceInstance};
use std::collections::HashMap;
use std::iter::Map;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Once;
use tokio::sync::RwLock;
use tokio::time::Duration;
use uuid::Uuid;

mod discovery;
mod node;

static ONCE: Once = Once::new();

lazy_static! {
    static ref INSTANCES: Arc<RwLock<Vec<ServiceInstance>>> = Arc::new(RwLock::new(Vec::new()));
    // static ref CLUSTER: Arc<Cluster<RestClusterNode>> = Arc::new(Cluster::new(10 * 1000));
    static ref CLUSTER_NODE_MAP: Arc<RwLock<HashMap<ServiceInstance,Arc<Cluster<TestNode>>>>> = Arc::new(RwLock::new(HashMap::new()));
}

#[tokio::test]
async fn test_cluster() {
    setup();

    let discovery_service = discovery::TestDiscoveryService::new(INSTANCES.clone());
    let discovery_client = DiscoveryClient::new(discovery_service);
    let cluster_1 = Arc::new(Cluster::<node::TestNode>::default());
    let config = ClusterConfig {
        connection_timeout: 100,
        election_timeout: 100,
        max_node: NonZeroUsize::new(10).unwrap(),
        min_node: NonZeroUsize::new(4).unwrap(),
    };
    tokio::spawn(start_cluster(
        cluster_1,
        discovery_client,
        config,
        TestNode::new,
        get_info,
    ));

    let discovery_service = discovery::TestDiscoveryService::new(INSTANCES.clone());
    let discovery_client = DiscoveryClient::new(discovery_service);
    let cluster_2 = Arc::new(Cluster::<node::TestNode>::default());
    let config = ClusterConfig {
        connection_timeout: 0,
        election_timeout: 0,
        max_node: NonZeroUsize::new(10).unwrap(),
        min_node: NonZeroUsize::new(4).unwrap(),
    };
    tokio::spawn(start_cluster(
        cluster_2,
        discovery_client,
        config,
        TestNode::new,
        get_info,
    ));

    tokio::time::sleep(Duration::from_millis(10000)).await;
}

fn new_service_instance() -> ServiceInstance {
    let id = Uuid::new_v4().to_string();
    ServiceInstance::new(
        Some(id.clone()),
        None,
        None,
        None,
        false,
        Some(format!("mpsc://{}", id)),
        HashMap::new(),
        None,
    )
}

fn setup() {
    ONCE.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter("trace")
            .try_init()
            .unwrap();
    });
}

pub async fn get_info(instance: ServiceInstance) -> anyhow::Result<(ClusterInfo, ServiceInstance)> {
    let read = INSTANCES.read().await;
    todo!()
}
