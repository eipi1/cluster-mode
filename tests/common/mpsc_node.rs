use std::fmt::{Display, Formatter};
use almost_raft::{ClusterNode, Message};
use async_trait::async_trait;
use log::{error, trace};
use rust_cloud_discovery::ServiceInstance;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;

// #[derive(Debug, Clone, PartialEq, Eq)]
// pub struct NodeId(String);
// impl Deref for NodeId {
//     type Target = String;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

type MpscNodeRx = Arc<RwLock<Receiver<almost_raft::Message<MpscNode>>>>;

#[derive(Debug, Clone)]
pub struct MpscNode {
    pub node_id: MpscNodeId,
    pub rx: MpscNodeRx,
    pub tx: Sender<almost_raft::Message<Self>>,
}

impl PartialEq for MpscNode {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id
    }
}

impl Eq for MpscNode {}

impl Hash for MpscNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_id.hash(state)
    }
}

impl MpscNode {
    pub fn create_new(node_id: String) -> (MpscNode, Sender<Message<MpscNode>>, MpscNodeRx) {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let rx = Arc::new(RwLock::from(rx));
        let node = MpscNode {
            node_id: MpscNodeId(node_id),
            rx: rx.clone(),
            tx: tx.clone(),
        };
        (node, tx, rx)
    }
    pub fn new_node_from(
        node_id: String,
        tx: Sender<Message<MpscNode>>,
        rx: MpscNodeRx,
    ) -> MpscNode {
        MpscNode {
            node_id: MpscNodeId(node_id),
            rx,
            tx,
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct MpscNodeId(String);

impl From<String> for MpscNodeId {
    fn from(value: String) -> Self {
        MpscNodeId(value)
    }
}

impl Display for MpscNodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[async_trait]
impl ClusterNode for MpscNode {
    type NodeType = MpscNode;
    type NodeIdType = MpscNodeId;

    async fn send_message(&self, msg: Message<Self::NodeType>) {
        trace!("sending message to [{}] - msg: {:?}", self.node_id, msg);
        if let Err(e) = self.tx.send(msg).await {
            error!("[{}] error sending message: {:?}", self.node_id, e);
        }
    }

    fn node_id(&self) -> &Self::NodeIdType {
        &self.node_id
    }
}
