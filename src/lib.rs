#![allow(dead_code)]

#[macro_export]
macro_rules! log_error {
    ($result:expr) => {
        if let Err(e) = $result {
            error!("{}", e.to_string());
        }
    };
}

use almost_raft::election::{raft_election, RaftElectionState};
use almost_raft::{Message, Node};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use http::{Error, Request};
use hyper::client::{Client, HttpConnector};
use hyper::Body;
use log::{debug, error, info, trace};

use rust_cloud_discovery::{DiscoveryClient, DiscoveryService, ServiceInstance};
use serde::{Deserialize, Serialize};

use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::result::Result::Err;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::sync::{mpsc, RwLock};
use tokio_stream::StreamExt;

/// Status of a node/instance
pub enum InstanceMode {
    /// Haven't joined to any cluster yet
    Inactive,
    /// Current node is acting as a primary/leader
    Primary,
    /// It's a Secondary node
    Secondary,
}

pub struct Cluster {
    self_id: String,
    mode: RwLock<InstanceMode>,
    update_interval: u64,
    self_: RwLock<Option<ServiceInstance>>,
    primaries: RwLock<HashSet<RestClusterNode>>,
    secondaries: RwLock<Arc<HashSet<RestClusterNode>>>,
    /// how many primaries
    n_primary: usize,
    raft_tx: RwLock<Option<Sender<Message<RestClusterNode>>>>,
}

impl Cluster {
    pub fn new(update_interval: u64) -> Self {
        Cluster {
            update_interval,
            ..Default::default()
        }
    }

    #[doc(hidden)]
    pub fn _new(mode: InstanceMode, secondaries: HashSet<RestClusterNode>) -> Self{
        Cluster{
            mode:RwLock::new(mode),
            secondaries: RwLock::new(Arc::new(secondaries)),
            ..Default::default()
        }

    }

    pub async fn secondaries(&self) -> Option<Arc<HashSet<RestClusterNode>>> {
        if self.is_primary().await {
            let guard = self.secondaries.read().await;
            Some(guard.clone())
        } else {
            info!("[node: {}] not a primary node", &self.self_id);
            None
        }
    }

    pub async fn primaries(&self) -> Option<HashSet<RestClusterNode>> {
        if self.is_secondary().await {
            let guard = self.primaries.read().await;
            Some(guard.clone())
        } else {
            info!("[node: {}] not a secondary node", &self.self_id);
            None
        }
    }

    #[inline]
    pub async fn is_primary(&self) -> bool {
        let guard = self.mode.read().await;
        matches!(*guard, InstanceMode::Primary)
    }

    #[inline]
    pub async fn is_secondary(&self) -> bool {
        let guard = self.mode.read().await;
        matches!(*guard, InstanceMode::Secondary)
    }

    #[inline]
    pub async fn is_active(&self) -> bool {
        let guard = self.mode.read().await;
        !matches!(*guard, InstanceMode::Inactive)
    }

    pub async fn accept_raft_request_vote(&self, requester_node_id: String, term: usize) {
        self.send_message_to_raft(Message::RequestVote {
            term,
            node_id: requester_node_id,
        })
        .await;
    }

    pub async fn accept_raft_request_vote_resp(&self, term: usize, vote: bool) {
        self.send_message_to_raft(Message::RequestVoteResponse { term, vote })
            .await;
    }

    pub async fn accept_raft_heartbeat(&self, leader_node_id: String, term: usize) {
        self.send_message_to_raft(Message::HeartBeat {
            leader_node_id,
            term,
        })
        .await;
    }

    async fn send_message_to_raft(&self, msg: Message<RestClusterNode>) {
        trace!(
            "[node: {}] sending messages to raft: {:?}",
            &self.self_id,
            &msg
        );
        let guard = self.raft_tx.read().await;
        if let Some(tx) = guard.as_ref() {
            let result = tx.send(msg).await;
            log_error!(result);
        }
    }
}

impl Default for Cluster {
    fn default() -> Self {
        Cluster {
            self_id: uuid::Uuid::new_v4().to_string(),
            mode: RwLock::from(InstanceMode::Inactive),
            update_interval: 10 * 1000,
            self_: Default::default(),
            primaries: Default::default(),
            secondaries: Default::default(),
            n_primary: 1,
            raft_tx: Default::default(),
        }
    }
}

pub async fn start_cluster<T: DiscoveryService>(
    cluster: Arc<Cluster>,
    discovery_service: DiscoveryClient<T>,
) {
    info!("[node: {}] starting cluster...", &cluster.self_id);
    let raft_tx_timeout = 15;

    let (tx, mut raft_rx) = mpsc::channel::<Message<RestClusterNode>>(20);

    let (raft, raft_tx) = RaftElectionState::init(
        cluster.self_id.clone(),
        30 * 1000,
        10 * 1000,
        500,
        vec![],
        tx.clone(),
        20,
        3,
    );

    {
        let mut write_guard = cluster.raft_tx.write().await;
        *write_guard = Some(raft_tx.clone());
    }

    info!("[node: {}] spawning raft election...", &cluster.self_id);
    tokio::spawn(raft_election(raft));

    let mut remaining_update_interval = cluster.update_interval;

    let client = Client::new();

    //todo reconfirm if map is needed or only set of node id is enough
    // map of service instance_id, RestClusterNode
    let mut discovered: HashMap<String, RestClusterNode> = HashMap::new();

    loop {
        trace!(
            "[node: {}] update timeout: {}",
            &cluster.self_id,
            &remaining_update_interval
        );
        //get message from raft or time to check discovery service
        let start_time = Instant::now();
        let raft_msg = tokio::time::timeout(
            Duration::from_millis(remaining_update_interval),
            raft_rx.recv(),
        )
        .await;

        if let Ok(msg) = raft_msg {
            //only control message is expected, other message should be handled through peer
            handle_control_message_from_raft(&cluster, &discovered, msg).await;
            remaining_update_interval = unsigned_subtract(
                remaining_update_interval,
                start_time.elapsed().as_millis() as u64,
            );
            continue;
        }
        remaining_update_interval = cluster.update_interval;

        trace!("[node: {}] calling discovery service.", &cluster.self_id);
        let instances = if let Ok(instance) = discovery_service.get_instances().await {
            instance
        } else {
            vec![]
        };

        debug!("discovered instances: {:?}", instances);

        // collect cluster info
        let mut requests = FuturesUnordered::new();
        let mut current_instances = HashSet::new();
        for instance in instances {
            let id;
            if instance.instance_id().is_some() {
                id = instance.instance_id().clone().unwrap();
            } else {
                //must have some identifier
                continue;
            }
            if discovered.contains_key(&id) //no need to get info if already discovered
                || instance.uri().is_none()
            {
                current_instances.insert(id);
                continue;
            }
            current_instances.insert(id);

            let request = Request::builder()
                .uri(format!("{}{}", instance.uri().clone().unwrap(), PATH_INFO))
                .body(Body::empty());
            // use FuturesUnordered for parallel requests
            requests.push(send_request(&client, request, instance));
        }

        let mut new_nodes = HashSet::new();
        while let Some(result) = requests.next().await {
            match result {
                Ok((resp, instance)) => {
                    let info = serde_json::from_slice::<ClusterInfo>(resp.as_ref());
                    trace!(
                        "[node: {}] cluster info {:?} from {:?}",
                        &cluster.self_id,
                        &info,
                        &instance
                    );
                    if let Ok(info) = info {
                        if info.node_id == cluster.self_id {
                            {
                                let mut guard = cluster.self_.write().await;
                                guard.replace(instance.clone());
                            }
                            //is it need to add self to raft? or only peers.
                            // Ans: Only peers, has self id to identify itself
                        }
                        let node = RestClusterNode::new(info.node_id, instance);
                        if cluster.self_id != node.node_id {
                            new_nodes.insert(node.inner.instance_id().clone().unwrap());
                            //todo handle failure
                            debug!("[node: {}] new node found: {:?}", &cluster.self_id, &node);
                            let result = raft_tx
                                .send_timeout(
                                    Message::ControlAddNode(node.clone()),
                                    Duration::from_millis(raft_tx_timeout),
                                )
                                .await;
                            log_error!(result);
                        }
                        discovered.insert(node.inner.instance_id().clone().unwrap(), node);
                    }
                }
                Err(err) => {
                    error!("[node: {}] error getting cluster info: {}", &cluster.self_id, err.to_string());
                }
            }
        }

        let mut removed_nodes = HashSet::new();
        // remove if not exists in newly discovered instance list
        for (key, val) in discovered.iter() {
            if !current_instances.contains(val.service_instance().instance_id().as_ref().unwrap()) {
                removed_nodes.insert(key.clone());
            }
        }

        if !new_nodes.is_empty() || !removed_nodes.is_empty() {
            //update secondaries
            let mut current = {
                let guard = cluster.secondaries.read().await;
                guard.clone().as_ref().clone()
            };
            for node in removed_nodes {
                let removed = discovered.remove(&node);
                if let Some(removed) = removed {
                    debug!("removing node: {:?}", &removed);
                    current.remove(&removed);
                    let result = raft_tx
                        .send_timeout(
                            Message::ControlRemoveNode(removed),
                            Duration::from_millis(raft_tx_timeout),
                        )
                        .await;
                    log_error!(result);
                }
            }
            for node in new_nodes {
                if let Some(node) = discovered.get(&node) {
                    current.insert(node.clone());
                }
            }
            {
                trace!("[node: {}] updating secondaries to: {:?}", &cluster.self_id, &current);
                let mut write_guard = cluster.secondaries.write().await;
                *write_guard = Arc::new(current);
            }
        }
    }
}

async fn send_request(
    client: &Client<HttpConnector>,
    request: Result<Request<Body>, Error>,
    instance: ServiceInstance,
) -> anyhow::Result<(Bytes, ServiceInstance)> {
    let request = request?;
    let resp = client.request(request).await?;
    let resp = hyper::body::to_bytes(resp).await?;
    Ok((resp, instance))
}

#[inline]
async fn handle_control_message_from_raft(
    cluster: &Arc<Cluster>,
    discovered: &HashMap<String, RestClusterNode>,
    msg: Option<Message<RestClusterNode>>,
) {
    info!(
        "[node: {}] control message from raft: {:?}",
        cluster.self_id, &msg
    );
    if let Some(Message::ControlLeaderChanged(node_id)) = msg {
        let mut node = None;
        for discovered_node in discovered.values() {
            if discovered_node.node_id == node_id {
                node = Some(discovered_node);
            }
        }
        if let Some(node) = node {
            info!("new primary: {:?}", node);
            let mode = if cluster.self_id == node_id {
                InstanceMode::Primary
            } else {
                InstanceMode::Secondary
            };
            {
                let mut write_guard = cluster.mode.write().await;
                *write_guard = mode;
            }
            let node = node.clone();
            let mut write_guard = cluster.primaries.write().await;
            write_guard.insert(node);
        } else {
            error!("Node not found in discovered list");
        }
    }
}

pub async fn get_cluster_info(cluster: Arc<Cluster>) -> ClusterInfo {
    let node = {
        let guard = cluster.self_.read().await;
        guard.as_ref().map(|x| x.to_owned())
    };
    ClusterInfo {
        instance: node,
        node_id: cluster.self_id.clone(),
        update_interval: cluster.update_interval,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterInfo {
    pub node_id: String,
    pub instance: Option<ServiceInstance>,
    pub update_interval: u64,
}

#[derive(Debug, Clone)]
pub struct RestClusterNode {
    pub(crate) node_id: String,
    pub(crate) inner: ServiceInstance,
}

impl RestClusterNode {
    pub fn new(node_id: String, instance: ServiceInstance) -> Self {
        Self {
            node_id,
            inner: instance,
        }
    }

    pub fn service_instance(&self) -> &ServiceInstance {
        &self.inner
    }

    pub(crate) async fn send_request_vote(
        &self,
        node_id: String,
        term: usize,
    ) -> anyhow::Result<()> {
        self.send_raft_request(format!(
            "{}{}/{}/{}",
            self.inner.uri().clone().unwrap(),
            PATH_RAFT_REQUEST_VOTE,
            node_id,
            term
        ))
        .await
    }

    pub(crate) async fn send_request_vote_response(
        &self,
        vote: bool,
        term: usize,
    ) -> anyhow::Result<()> {
        self.send_raft_request(format!(
            "{}{}/{}/{}",
            self.inner.uri().clone().unwrap(),
            PATH_RAFT_VOTE,
            term,
            vote
        ))
        .await
    }

    pub(crate) async fn send_heartbeat(
        &self,
        leader_node_id: String,
        term: usize,
    ) -> anyhow::Result<()> {
        self.send_raft_request(format!(
            "{}{}/{}/{}",
            self.inner.uri().clone().unwrap(),
            PATH_RAFT_HEARTBEAT,
            leader_node_id,
            term
        ))
        .await
    }

    async fn send_raft_request(&self, uri: String) -> anyhow::Result<()> {
        trace!(
            "sending raft request to node: {}, path: {}",
            &self.node_id,
            &uri
        );
        let request = Request::builder().uri(uri).body(Body::empty())?;
        //todo use pooled connection
        let client = Client::new();
        let resp = client.request(request).await?;
        let resp = hyper::body::to_bytes(resp).await?;
        trace!(
            "raft request response: {:?}",
            std::str::from_utf8(resp.as_ref())
        );
        Ok(())
    }
}

#[async_trait]
impl Node for RestClusterNode {
    type NodeType = RestClusterNode;

    async fn send_message(&self, msg: Message<Self::NodeType>) {
        debug!(
            "[RestClusterNode: {}] message from raft: {:?}",
            &self.node_id, &msg
        );
        match msg {
            Message::RequestVote { node_id, term } => {
                let result = self.send_request_vote(node_id, term).await;
                log_error!(result);
            }
            Message::RequestVoteResponse { vote, term } => {
                let result = self.send_request_vote_response(vote, term).await;
                log_error!(result);
            }
            Message::HeartBeat {
                leader_node_id,
                term,
            } => {
                let result = self.send_heartbeat(leader_node_id, term).await;
                log_error!(result);
            }
            _ => {}
        }
    }

    fn node_id(&self) -> &String {
        &self.node_id
    }
}

impl PartialEq for RestClusterNode {
    fn eq(&self, other: &Self) -> bool {
        self.node_id.eq(&other.node_id)
    }
}

impl Eq for RestClusterNode {}

impl Hash for RestClusterNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_id.hash(state);
    }
}

const PATH_INFO: &str = "/cluster/info";
/// /cluster/raft/request-vote/{requester_node_id}/{term}
const PATH_RAFT_REQUEST_VOTE: &str = "/cluster/raft/request-vote";
/// /cluster/raft/vote/{term}/{true|false}
const PATH_RAFT_VOTE: &str = "/cluster/raft/vote";
/// /cluster/raft/beat/{leader_node_id}/{term}
const PATH_RAFT_HEARTBEAT: &str = "/cluster/raft/beat";

/// subtract unsigned number, if negative, return 0.
#[inline(always)]
fn unsigned_subtract<T>(lhs: T, rhs: T) -> T
where
    T: PartialEq + PartialOrd + std::ops::Sub<Output = T> + From<u64>,
{
    if lhs < rhs {
        0.into()
    } else {
        lhs - rhs
    }
}

#[cfg(test)]
mod test {
    use crate::{start_cluster, Cluster};
    use cloud_discovery_kubernetes::KubernetesDiscoverService;
    use rust_cloud_discovery::DiscoveryClient;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_cluster_impl() {
        let result = KubernetesDiscoverService::init("overload".to_string(), "default".to_string())
            .await;
        if let Ok(k8s) = result {
            let cluster = Arc::new(Cluster::default());
            let client = DiscoveryClient::new(k8s);
            tokio::spawn(start_cluster(cluster, client));
        }

    }
    #[test]
    fn misc() {}
}
