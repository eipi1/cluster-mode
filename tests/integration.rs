#![allow(unused_variables)]
#![allow(unused_imports)]
use crate::common::{MpscDiscoveryService, MpscNode, MpscNodeId};
use almost_raft::Message;
use cluster_mode::{start_cluster, Cluster, ClusterConfig, ClusterInfo, ClusterInstanceId};
use lazy_static::lazy_static;
use log::{error, trace};
use rust_cloud_discovery::{DiscoveryClient, DiscoveryService, ServiceInstance};
use std::collections::HashMap;
use std::future::Future;
use std::iter::Map;
use std::num::NonZeroUsize;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::Once;
use std::sync::RwLock as StdRwLock;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::time::error::Elapsed;
use tokio::time::{Duration, Timeout};
use uuid::Uuid;

mod common;

static ONCE: Once = Once::new();
type MpscNodeRx = Arc<RwLock<Receiver<almost_raft::Message<MpscNode>>>>;
type MpscNodeTx = Sender<Message<MpscNode>>;

lazy_static! {
    // static ref INSTANCES: Arc<RwLock<Vec<ServiceInstance>>> = Arc::new(RwLock::new(Vec::new()));
    static ref NODE_ID_NODE_MAP: StdRwLock<HashMap<MpscNodeId, (MpscNodeRx, MpscNodeTx)>> =
        StdRwLock::new(HashMap::new());
    static ref SERVICE_INSTANCE_TO_CLUSTER: StdRwLock<HashMap<String, Arc<Cluster<MpscNode>>>> =
        StdRwLock::new(HashMap::new());

}

#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_cluster() {
    setup();

    let config = ClusterConfig {
        connection_timeout: 10,
        election_timeout: 100,
        update_interval: 300,
        max_node: NonZeroUsize::new(10).unwrap(),
        min_node: NonZeroUsize::new(4).unwrap(),
    };

    let mut discovery_service = common::MpscDiscoveryService::new();

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance = create_cluster_instance(config.clone(), discovery_client).await;

    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance = create_cluster_instance(config.clone(), discovery_client).await;
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    tokio::time::sleep(Duration::from_secs(1000)).await;

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance = create_cluster_instance(config.clone(), discovery_client).await;
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    tokio::time::sleep(Duration::from_secs(1000)).await;
    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance = create_cluster_instance(config.clone(), discovery_client).await;
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }

    tokio::time::sleep(Duration::from_secs(100000)).await;
}

async fn create_cluster_instance(
    config: ClusterConfig,
    // mut discovery_service: MpscDiscoveryService,
    discovery_client: DiscoveryClient<MpscDiscoveryService>,
) -> Arc<Cluster<MpscNode>> {
    let cluster_instance = Arc::new(Cluster::<MpscNode>::default());
    tokio::spawn(start_cluster(
        cluster_instance.clone(),
        discovery_client,
        config,
        new_node_from_service_instance,
        get_info,
    ));

    let cluster_id = cluster_instance.get_id();
    let (node, tx, rx) = MpscNode::create_new(cluster_id.to_string());
    // let node_id = node.node_id.deref().clone();
    NODE_ID_NODE_MAP
        .write()
        .unwrap()
        .insert(cluster_id.clone(), (rx.clone(), tx.clone()));
    tokio::spawn(start_message_handler(rx, cluster_instance.clone()));
    // discovery_service
    //     .add_node(ServiceInstance::new(
    //         Some(node_id.clone()),
    //         None,
    //         None,
    //         None,
    //         false,
    //         Some(mpsc_uri(node_id)),
    //         HashMap::default(),
    //         None,
    //     ))
    //     .await;
    cluster_instance
}

async fn start_message_handler(
    rx: Arc<RwLock<Receiver<Message<MpscNode>>>>,
    // mut rx: Receiver<Message<MpscNode>>,
    cluster: Arc<Cluster<MpscNode>>,
) {
    loop {
        let result = tokio::time::timeout(Duration::from_millis(50), rx.write().await.recv()).await;
        if let Ok(Some(msg)) = result {
            trace!("[{}] got message - {:?}", cluster.get_id().to_string(), &msg);
            match msg {
                Message::RequestVote {
                    requester_node_id,
                    term,
                } => {
                    cluster
                        .accept_raft_request_vote(requester_node_id.to_string(), term)
                        .await;
                }
                Message::RequestVoteResponse { vote, term } => {
                    cluster.accept_raft_request_vote_resp(term, vote).await;
                }
                Message::HeartBeat {
                    leader_node_id,
                    term,
                } => {
                    cluster
                        .accept_raft_heartbeat(leader_node_id.to_string(), term)
                        .await;
                }
                _ => {
                    error!("Unexpected message")
                }
            }
        }
    }
}

// async fn wait_until<T>(x: &mut T) -> Result<T::Output, Elapsed>
// where T: Future {
//     x.await
// }

// fn mpsc_uri(node_id: String) -> String {
//     format!("mpsc://{}", node_id)
// }

pub fn new_node_from_service_instance(
    node_id: String,
    service_instance: ServiceInstance,
) -> MpscNode {
    let (rx, tx) = NODE_ID_NODE_MAP
        .read()
        .unwrap()
        .get(&(node_id.clone().into()))
        .unwrap()
        .clone();
    MpscNode::new_node_from(node_id, tx, rx)
}

fn setup() {
    ONCE.call_once(|| {
        env_logger::init_from_env(
            env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "trace"),
        );
    });
}

pub async fn get_info(instance: ServiceInstance) -> anyhow::Result<(ClusterInfo, ServiceInstance)> {
    let id = instance.instance_id().clone().unwrap();
    let cluster = SERVICE_INSTANCE_TO_CLUSTER
        .read()
        .unwrap()
        .get(&id)
        .unwrap()
        .clone();
    let info = ClusterInfo {
        node_id: cluster.get_id().to_string(),
        instance: Some(instance.clone()),
    };
    Ok((info, instance))
}
