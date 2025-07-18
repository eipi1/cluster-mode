#![allow(unused_variables)]
#![allow(unused_imports)]
#![allow(clippy::mutable_key_type)]
use crate::common::{MpscDiscoveryService, MpscNode, MpscNodeId};
use almost_raft::Message;
use cluster_mode::{start_cluster, Cluster, ClusterConfig, ClusterInfo, ClusterInstanceId};
use lazy_static::lazy_static;
use log::{error, info, trace};
use rust_cloud_discovery::{DiscoveryClient, DiscoveryService, ServiceInstance};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
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
#[ignore]
async fn test_mpsc_cluster() {
    setup();

    let config = ClusterConfig {
        connection_timeout: 10,
        election_timeout: 100,
        update_interval: 100,
        max_node: NonZeroUsize::new(10).unwrap(),
        min_node: NonZeroUsize::new(4).unwrap(),
    };

    let mut cluster_instances = vec![];

    let discovery_service = common::MpscDiscoveryService::new();

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
    cluster_instances.push(cluster_instance);

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
    cluster_instances.push(cluster_instance);

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
    cluster_instances.push(cluster_instance);

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
    cluster_instances.push(cluster_instance);

    tokio::time::sleep(Duration::from_secs(4)).await;
    // let primaries = cluster_instance_4.primaries().await;
    // let secondaries = cluster_instance_4.secondaries().await;
    assert!(
        cluster_instances[0].primaries().await.is_some()
            || cluster_instances[0].secondaries().await.is_some()
    );

    //get primary & secondary for later tests
    let (primaries, _) = get_primaries_and_secondaries(&cluster_instances).await;
    let primaries = primaries.unwrap();
    let primary = primaries.iter().next().unwrap();

    // add new nodes, should be connected to cluster as secondaries
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
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(cluster_instance.is_secondary().await);
    cluster_instances.push(cluster_instance);

    let mut primaries_after = None;
    let mut secondaries_after = None;
    for cluster_instance in &cluster_instances {
        if cluster_instance.is_primary().await {
            secondaries_after = cluster_instance.secondaries().await;
        } else {
            primaries_after = cluster_instance.primaries().await;
        }
    }
    let primaries_after = primaries_after.unwrap();
    let primaries_after = primaries_after.iter().next().unwrap();
    let secondaries_after = secondaries_after.unwrap();
    assert_eq!(primary, primaries_after);
    assert_eq!(secondaries_after.len(), 4)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_mpsc_cluster_increase_nodes() {
    // setup();

    let config = ClusterConfig {
        connection_timeout: 10,
        election_timeout: 100,
        update_interval: 100,
        max_node: NonZeroUsize::new(10).unwrap(),
        min_node: NonZeroUsize::new(4).unwrap(),
    };

    let mut cluster_instances = vec![];

    let discovery_service = common::MpscDiscoveryService::new();

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance =
        create_cluster_instance_with_id(config.clone(), discovery_client, "1").await;

    // let service_instance_id = "1".to_string();
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    cluster_instances.push(cluster_instance);

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance =
        create_cluster_instance_with_id(config.clone(), discovery_client, "2").await;
    // let service_instance_id = "2".to_string();
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    cluster_instances.push(cluster_instance);

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance =
        create_cluster_instance_with_id(config.clone(), discovery_client, "3").await;
    // let service_instance_id = "3".to_string();
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    cluster_instances.push(cluster_instance);

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance =
        create_cluster_instance_with_id(config.clone(), discovery_client, "4").await;
    // let service_instance_id = "4".to_string();
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    cluster_instances.push(cluster_instance);

    tokio::time::sleep(Duration::from_secs(4)).await;
    // let primaries = cluster_instance_4.primaries().await;
    // let secondaries = cluster_instance_4.secondaries().await;
    assert!(
        cluster_instances[0].primaries().await.is_some()
            || cluster_instances[0].secondaries().await.is_some()
    );

    //get primary & secondary for later tests
    // let mut primaries=None;
    // let mut secondaries=None;
    let (primaries, secondaries) = get_primaries_and_secondaries(&cluster_instances).await;
    let primaries = primaries.unwrap();
    let primary = primaries.iter().next().unwrap();

    // We have 3 secondaries, add 3 new nodes
    setup();
    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance =
        create_cluster_instance_with_id(config.clone(), discovery_client, "5").await;
    // let service_instance_id = "5".to_string();
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    cluster_instances.push(cluster_instance);

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance =
        create_cluster_instance_with_id(config.clone(), discovery_client, "6").await;
    // let service_instance_id = "6".to_string();
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    cluster_instances.push(cluster_instance);

    tokio::time::sleep(Duration::from_secs(3)).await;
    // assert!(cluster_instance.is_secondary().await);

    let (primaries_after, secondaries_after) =
        get_primaries_and_secondaries(&cluster_instances).await;
    let primaries_after = primaries_after.unwrap();
    let primaries_after = primaries_after.iter().next().unwrap();
    let secondaries_after = secondaries_after.unwrap();
    // assert_eq!(primary, primaries_after); //todo to be fixed by pre-vote
    assert_eq!(secondaries_after.len(), 5);

    let discovery_client = DiscoveryClient::new(discovery_service.clone());
    let cluster_instance =
        create_cluster_instance_with_id(config.clone(), discovery_client, "7").await;
    // let service_instance_id = "6".to_string();
    let service_instance_id = uuid::Uuid::new_v4().to_string();
    discovery_service.register(service_instance_id.clone());
    {
        SERVICE_INSTANCE_TO_CLUSTER
            .write()
            .unwrap()
            .insert(service_instance_id, cluster_instance.clone());
    }
    cluster_instances.push(cluster_instance);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let primary = primaries_after;
    let (primaries_after, secondaries_after) =
        get_primaries_and_secondaries(&cluster_instances).await;
    let primaries_after = primaries_after.unwrap();
    let primaries_after = primaries_after.iter().next().unwrap();
    let secondaries_after = secondaries_after.unwrap();
    assert_eq!(primary, primaries_after);
    assert_eq!(secondaries_after.len(), 6);


}

async fn get_primaries_and_secondaries(
    cluster_instances: &Vec<Arc<Cluster<MpscNode>>>,
) -> (Option<HashSet<MpscNode>>, Option<Arc<HashSet<MpscNode>>>) {
    let mut primaries_after = None;
    let mut secondaries_after = None;
    for cluster_instance in cluster_instances {
        if cluster_instance.is_primary().await {
            //there should be only one primary
            assert_eq!(secondaries_after, None, "there should be only one primary");
            secondaries_after = cluster_instance.secondaries().await;
        } else {
            primaries_after = cluster_instance.primaries().await;
        }
    }
    (primaries_after, secondaries_after)
}

async fn create_cluster_instance(
    config: ClusterConfig,
    // mut discovery_service: MpscDiscoveryService,
    discovery_client: DiscoveryClient<MpscDiscoveryService>,
) -> Arc<Cluster<MpscNode>> {
    create_cluster_instance_with_id(
        config,
        discovery_client,
        uuid::Uuid::new_v4().to_string().as_str(),
    )
    .await
}

async fn create_cluster_instance_with_id(
    config: ClusterConfig,
    // mut discovery_service: MpscDiscoveryService,
    discovery_client: DiscoveryClient<MpscDiscoveryService>,
    id: &str,
) -> Arc<Cluster<MpscNode>> {
    let cluster_instance = Arc::new(Cluster::<MpscNode>::new(MpscNodeId::from(id.to_string())));
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
            trace!("[node: {}] got message - {:?}", cluster.get_id(), &msg);
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

pub fn new_node_from_service_instance(
    node_id: MpscNodeId,
    service_instance: ServiceInstance,
) -> MpscNode {
    let (rx, tx) = NODE_ID_NODE_MAP
        .read()
        .unwrap()
        .get(&(node_id.clone()))
        .unwrap()
        .clone();
    MpscNode::new_node_from(node_id, tx, rx)
}

fn setup() {
    ONCE.call_once(|| {
        env_logger::Builder::from_env(
            env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "trace"),
        )
        .format_timestamp(Some(env_logger::TimestampPrecision::Millis))
        .init();
    });
}

pub async fn get_info(
    instance: ServiceInstance,
) -> anyhow::Result<(ClusterInfo<MpscNode>, ServiceInstance)> {
    let id = instance.instance_id().clone().unwrap();
    let cluster = SERVICE_INSTANCE_TO_CLUSTER
        .read()
        .unwrap()
        .get(&id)
        .unwrap()
        .clone();
    let info = ClusterInfo {
        node_id: cluster.get_id().clone(),
        instance: Some(instance.clone()),
    };
    Ok((info, instance))
}
