use almost_raft::{Message, Node};
use async_trait::async_trait;
use rust_cloud_discovery::ServiceInstance;
use uuid::Uuid;
use cluster_mode::ClusterInfo;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug)]
pub struct TestNode {
    id: String,
    inner: ServiceInstance,
}

impl PartialEq for TestNode {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl Eq for TestNode{}

impl Hash for TestNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}

impl TestNode {
    pub fn new(id: String, inner: ServiceInstance) -> TestNode {
        Self { id, inner }
    }
}

#[async_trait]
impl Node for TestNode {
    type NodeType = TestNode;

    async fn send_message(&self, msg: Message<Self::NodeType>) {
        match msg {
            Message::RequestVote { node_id, term } => {
                todo!()
            }
            Message::RequestVoteResponse { vote, term } => {
                todo!()
            }
            Message::HeartBeat {
                leader_node_id,
                term,
            } => {
                todo!()
            }
            _ => {}
        }
    }

    fn node_id(&self) -> &String {
        &self.id
    }
}
