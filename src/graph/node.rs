#![allow(unused_variables)]
#![allow(dead_code)]

use crate::query::SqlType;
use futures::{io::Cursor, AsyncBufRead, AsyncRead, AsyncSeek};
use std::{fmt::Debug, sync::Arc};

// Define nodes in the execution graph with definitions based in relational alebra
// https://en.wikipedia.org/wiki/Relational_algebra

#[derive(Debug, Clone)]
pub enum OpType {
    Nop,
    Rename,
    Reorder,
    Project,
    Select,
    Set,
    Join,
    Agg,
}

#[derive(Debug)]
pub enum SetOpType {
    Union,
    Difference,
    Product,
}

#[derive(Debug)]
pub enum JoinOpType {
    Natural,
    Theta,
    Equi,
    Anti,
    Division,
    LeftOuter,
    RightOuter,
    FullOuter,
}

#[derive(Debug)]
pub enum AggOpType {
    Sum,
    Count,
    Average,
    Maximum,
    Minimum,
}

#[derive(Debug, Clone)]
pub enum IoType {
    Ram,
    Disk,
    Network,
    Generator,
}
#[derive(Debug, Clone)]
pub enum NodeType {
    Nop,
    Leaf(IoType),
    Op(OpType),
}

#[derive(Debug, Clone)]
pub enum NodeInput {
    None,
    Single(Arc<HyperNode>),
    Double(Arc<HyperNode>, Arc<HyperNode>),
    Leaf,
}

#[derive(Debug, Clone)]
pub enum NodeOutput {
    None,
    Single(Arc<HyperNode>),
    Root,
}

#[derive(Clone, Debug)]
pub struct WorkNodeCursor {
    work_nodes: Vec<WorkNode>,
    cursor: Cursor<Vec<u8>>,
}

impl WorkNodeCursor {
    pub fn new(work_nodes: Vec<WorkNode>) -> WorkNodeCursor {
        WorkNodeCursor {
            work_nodes,
            cursor: Cursor::new(Vec::new()),
        }
    }
}

impl AsyncRead for WorkNodeCursor {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        todo!()
    }
}

impl AsyncBufRead for WorkNodeCursor {
    fn poll_fill_buf(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<&[u8]>> {
        todo!()
    }

    fn consume(self: std::pin::Pin<&mut Self>, amt: usize) {
        todo!()
    }
}
impl AsyncSeek for WorkNodeCursor {
    fn poll_seek(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        pos: std::io::SeekFrom,
    ) -> std::task::Poll<std::io::Result<u64>> {
        todo!()
    }
}

pub trait Node {
    fn input(&self) -> NodeInput;
    fn output(&self) -> NodeOutput;
    fn personality(&self) -> NodeType;

    fn curse(&self) -> Arc<WorkNodeCursor>;
}

#[derive(Debug)]
struct NodeInfo {
    input: NodeInput,
    output: NodeOutput,
    personality: NodeType,
}

#[derive(Clone, Debug)]
pub struct RootNode {
    query_id: i64,
    graph: Option<Arc<HyperNode>>,
}

#[derive(Clone, Debug)]
pub struct HyperNode {
    name: String,
    columns: Option<Vec<String>>,
    info: Arc<NodeInfo>,
}

#[derive(Clone, Debug)]
pub struct WorkNode {
    parition: i64,
    placement: String,
    info: Arc<NodeInfo>,
}

pub struct GraphBuilder {
    query_id: i64,
    root: Arc<RootNode>,
}

impl HyperNode {
    fn new(name: String, columns: Option<Vec<String>>, info: NodeInfo) -> HyperNode {
        HyperNode {
            name,
            columns,
            info: Arc::new(info),
        }
    }
}

impl Node for HyperNode {
    fn input(&self) -> NodeInput {
        self.info.input.clone()
    }

    fn output(&self) -> NodeOutput {
        self.info.output.clone()
    }

    fn personality(&self) -> NodeType {
        self.info.personality.clone()
    }

    fn curse(&self) -> Arc<WorkNodeCursor> {
        let work_nodes = vec![];
        Arc::new(WorkNodeCursor::new(work_nodes))
    }
}

impl RootNode {
    pub fn new(query_id: i64) -> RootNode {
        RootNode {
            query_id,
            graph: None,
        }
    }
}

impl Node for RootNode {
    fn input(&self) -> NodeInput {
        match self.graph {
            Some(ref root) => root.input(),
            None => NodeInput::None,
        }
    }

    fn output(&self) -> NodeOutput {
        match self.graph {
            Some(ref root) => root.output(),
            None => NodeOutput::None,
        }
    }

    fn personality(&self) -> NodeType {
        match self.graph {
            Some(ref root) => root.personality(),
            None => NodeType::Nop,
        }
    }

    fn curse(&self) -> Arc<WorkNodeCursor> {
        todo!()
    }
}

pub enum BuilderNode {
    Root(Arc<RootNode>),
    Rename(Arc<BuilderNode>, Vec<(String, String)>),
    Reorder(Arc<BuilderNode>, Vec<(String, String)>),
    Select(Arc<BuilderNode>, String),
    Project(Arc<BuilderNode>, Vec<String>),
    Join(
        JoinOpType,
        Arc<BuilderNode>,
        Arc<BuilderNode>,
        Arc<dyn Fn(Box<dyn SqlType>, Box<dyn SqlType>) -> bool>,
    ),
    Agg(
        AggOpType,
        Arc<BuilderNode>,
        Vec<(String, String)>,
        Arc<dyn Fn(Box<dyn SqlType>, Box<dyn SqlType>) -> Box<dyn SqlType>>,
    ),
}

impl GraphBuilder {
    fn new(query_id: i64) -> GraphBuilder {
        GraphBuilder {
            query_id,
            root: Arc::new(RootNode::new(query_id)),
        }
    }

    fn add_subselect(
        self: &mut Self,
        project_columns: Option<Vec<String>>,
        input_relation: String,
    ) -> &mut Self {

        // First: setup where the data comes from
        let mut select_node = Arc::new(HyperNode::new(
            format!("select_{}", input_relation),
            None,
            NodeInfo {
                input: NodeInput::Leaf,
                output: NodeOutput::Root,
                personality: NodeType::Leaf(IoType::Ram),
            },
        ));

        // Second: Project out the data that we want to use
        let mut project_node = Arc::new( HyperNode::new(
            String::from("project"),
            project_columns.clone(),
            NodeInfo {
                input: NodeInput::Single(select_node.clone()),
                output: NodeOutput::Root,
                personality: NodeType::Op(OpType::Project),
            }
        ));
        let select_node = unsafe { Arc::get_mut_unchecked(&mut select_node) };
        let select_node_info =  unsafe { Arc::get_mut_unchecked(&mut select_node.info) };
        select_node_info.output = NodeOutput::Single(project_node.clone());

        // Third: Reorder the data that we want output by the subselect
        let reorder_node = Arc::new( HyperNode::new(
            String::from("reorder"),
            project_columns,
            NodeInfo {
                input: NodeInput::Single(project_node.clone()),
                output: NodeOutput::Root,
                personality: NodeType::Op(OpType::Reorder),
            }
        ));
        let project_node = unsafe { Arc::get_mut_unchecked(&mut project_node) };
        let project_node_info =  unsafe { Arc::get_mut_unchecked(&mut project_node.info) };
        project_node_info.output = NodeOutput::Single(reorder_node.clone());

        // TODO: Nested subselects and joins as recursive nodes in execution graph
        // Hook up the root to the reorder
        let mut root = unsafe { Arc::get_mut_unchecked(&mut self.root) };
        root.graph = Some(reorder_node);

        self
    }

    fn build(&mut self) -> Arc<RootNode> {
        self.root.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenv::dotenv;
    use lazy_static::lazy_static;
    use nom_sql::parser::parse_query;

    lazy_static! {
        static ref FIXTURE: () = {
            dotenv().ok();
            env_logger::init();
            ()
        };
    }

    pub fn setup() {
        lazy_static::initialize(&FIXTURE);
    }

    #[actix_rt::test]
    async fn test_can_inflate_select_star() {
        setup();

        let query = "SELECT * FROM FOO";
        let sql_query = parse_query(query).expect("Failed to parse test query");
        log::trace!("Inflating executing graph for {:?}", sql_query);

        let root = GraphBuilder::new(1)
            .add_subselect(None, String::from("foo"))
            .build();

        log::trace!("Inflated execution graph: {:?}", root.as_ref());
    }
}
