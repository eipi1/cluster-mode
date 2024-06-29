#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::discovery::SimpleVecDiscoveryService;
use crate::mpsc_node::MpscNode;
use crate::node::TestNode;
use almost_raft::Message;
use cluster_mode::{start_cluster, Cluster, ClusterConfig, ClusterInfo};
use lazy_static::lazy_static;
use log::{error, trace};
use rust_cloud_discovery::{DiscoveryClient, ServiceInstance};
use std::collections::HashMap;
use std::iter::Map;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Once;
use std::sync::RwLock as StdRwLock;
use tokio::sync::mpsc::Receiver;
use tokio::sync::RwLock;
use tokio::time::Duration;
use uuid::Uuid;

mod discovery;
mod mpsc_discovery;
mod mpsc_node;
mod node;

static ONCE: Once = Once::new();

lazy_static! {
    static ref INSTANCES: Arc<RwLock<Vec<ServiceInstance>>> = Arc::new(RwLock::new(Vec::new()));
    static ref NODE_ID_NODE_MAP: StdRwLock<HashMap<String, MpscNode>> =
        StdRwLock::new(HashMap::new());
    static ref NODE_ID_SERVICE_INSTANCE_MAP: StdRwLock<HashMap<String, ServiceInstance>> =
        StdRwLock::new(HashMap::new());
}

// #[tokio::test(flavor = "multi_thread")]
#[tokio::test]
async fn test_mpsc_cluster() {
    setup();

    let config = ClusterConfig {
        connection_timeout: 10,
        election_timeout: 100,
        update_interval: 1000,
        max_node: NonZeroUsize::new(10).unwrap(),
        min_node: NonZeroUsize::new(4).unwrap(),
    };

    let mut discovery_service = discovery::SimpleVecDiscoveryService::new();

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance_1 =
        create_cluster_instance(config.clone(), discovery_service.clone(), discovery_client).await;

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance_2 =
        create_cluster_instance(config.clone(), discovery_service.clone(), discovery_client).await;

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance_3 =
        create_cluster_instance(config.clone(), discovery_service.clone(), discovery_client).await;

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance_4 =
        create_cluster_instance(config.clone(), discovery_service.clone(), discovery_client).await;
    tokio::time::sleep(Duration::from_secs(100000)).await;
}

async fn create_cluster_instance(
    config: ClusterConfig,
    mut discovery_service: SimpleVecDiscoveryService,
    discovery_client: DiscoveryClient<SimpleVecDiscoveryService>,
) -> Arc<Cluster<MpscNode>> {
    let cluster_instance = Arc::new(Cluster::<mpsc_node::MpscNode>::default());
    tokio::spawn(start_cluster(
        cluster_instance.clone(),
        discovery_client,
        config,
        new_node_from_service_instance,
        get_info,
    ));
    let (node, tx, rx) = MpscNode::create_new(cluster_instance.get_id().clone());
    let node_id = node.node_id.deref().clone();
    NODE_ID_NODE_MAP
        .write()
        .unwrap()
        .insert(node_id.clone(), node.clone());
    tokio::spawn(message_handler(rx, node.clone(), cluster_instance.clone()));
    discovery_service
        .add_node(ServiceInstance::new(
            Some(node_id.clone()),
            None,
            None,
            None,
            false,
            Some(mpsc_uri(node_id)),
            HashMap::default(),
            None,
        ))
        .await;
    cluster_instance
}

async fn message_handler(
    rx: Arc<RwLock<Receiver<Message<MpscNode>>>>,
    node: MpscNode,
    cluster: Arc<Cluster<MpscNode>>,
) {
    loop {
        let result = tokio::time::timeout(Duration::from_millis(50), rx.write().await.recv()).await;
        if let Ok(Some(msg)) = result {
            trace!("[{}] got message - {:?}", node.node_id.deref(), &msg);
            match msg {
                Message::RequestVote {
                    requester_node_id,
                    term,
                } => {
                    cluster
                        .accept_raft_request_vote(requester_node_id, term)
                        .await;
                }
                Message::RequestVoteResponse { vote, term } => {
                    cluster.accept_raft_request_vote_resp(term, vote).await;
                }
                Message::HeartBeat {
                    leader_node_id,
                    term,
                } => {
                    cluster.accept_raft_heartbeat(leader_node_id, term).await;
                }
                _ => {
                    error!("Unexpected message")
                }
            }
        }
    }
}

fn mpsc_uri(node_id: String) -> String {
    format!("mpsc://{}", node_id)
}

pub fn new_node_from_service_instance(
    node_id: String,
    service_instance: ServiceInstance,
) -> MpscNode {
    NODE_ID_NODE_MAP
        .read()
        .unwrap()
        .get(&node_id)
        .unwrap()
        .clone()
    // MpscNode::create_new()
}

// fn new_service_instance() -> ServiceInstance {
//     let id = Uuid::new_v4().to_string();
//     ServiceInstance::new(
//         Some(id.clone()),
//         None,
//         None,
//         None,
//         false,
//         Some(format!("map://{}", id)),
//         HashMap::new(),
//         None,
//     )
// }

fn setup() {
    ONCE.call_once(|| {
        env_logger::init_from_env(
            env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "trace"),
        );
    });
}

pub async fn get_info(instance: ServiceInstance) -> anyhow::Result<(ClusterInfo, ServiceInstance)> {
    let uri = instance.uri().as_ref().unwrap();
    let split = uri.split("://").collect::<Vec<_>>();
    // let node = NODE_ID_NODE_MAP.read().unwrap().get(split[1]).unwrap().clone();
    let info = ClusterInfo {
        node_id: split[1].to_string(), // node id & service instance id are same
        instance: Some(instance.clone()),
    };
    return Ok((info, instance));
}
