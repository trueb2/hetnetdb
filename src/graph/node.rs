#![allow(unused_variables)]
#![allow(dead_code)]

use std::{fmt::Debug, sync::Arc};

use futures::{io::Cursor, AsyncBufRead, AsyncRead, AsyncSeek};

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
pub enum AggType {
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
    Single(HyperNode),
    Double(HyperNode, HyperNode),
}

#[derive(Debug, Clone)]
pub enum NodeOutput {
    None,
    Single(HyperNode),
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
    graph: Option<HyperNode>,
}

#[derive(Clone, Debug)]
pub struct HyperNode {
    name: String,
    columns: Vec<String>,
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
    fn new(name: String, columns: Vec<String>, info: NodeInfo) -> HyperNode {
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

impl GraphBuilder {
    fn new(query_id: i64) -> GraphBuilder {
        GraphBuilder {
            query_id,
            root: Arc::new(RootNode::new(query_id)),
        }
    }

    fn build(&mut self) -> Arc<RootNode> {
        self.root.clone()
    }
}
