#![allow(unused_variables)]
#![allow(dead_code)]

use crate::tables;
use crate::{
    error_handler::CustomError,
    query::{QueryRecord, SqlType},
    AppData,
};
use async_trait::async_trait;
use futures::sink::*;
use futures::stream::*;
use futures::{channel::mpsc::Receiver, channel::mpsc::Sender, lock::Mutex};
use nom_sql::{FunctionExpression, SelectStatement, SqlQuery};
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
    Ram(String),
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

#[derive(Clone)]
pub struct ExecuteContext {
    pub user_id: i64,
    pub app_data: Arc<AppData>,
}

#[async_trait]
pub trait Node {
    fn input(&self) -> NodeInput;
    fn personality(&self) -> NodeType;
    async fn curse(
        &self,
        ctx: Arc<ExecuteContext>,
        sender: Sender<Result<QueryRecord, CustomError>>,
    ) -> Result<(), CustomError>;
}

#[derive(Debug)]
struct NodeInfo {
    input: NodeInput,
    personality: NodeType,
}

#[derive(Clone, Debug)]
struct ExecutionInfo {
    upstream: Arc<Receiver<Result<QueryRecord, CustomError>>>,
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
    execution_info: Arc<Mutex<Option<ExecutionInfo>>>,
}

#[derive(Clone)]
pub struct WorkNode {
    ctx: Arc<ExecuteContext>,
    placement: Placement,
    info: Arc<NodeInfo>,
}

#[derive(Clone, Debug)]
pub enum Partition {
    Whole,
    Partial(u64, u64),
}

#[derive(Clone, Debug)]
pub enum Placement {
    Server(Partition), // TG
    Edge(Partition),   // TU
}

pub struct GraphBuilder {
    query_id: i64,
    root: Arc<RootNode>,
}

impl WorkNode {
    fn new(ctx: Arc<ExecuteContext>, placement: Placement, info: Arc<NodeInfo>) -> WorkNode {
        WorkNode {
            ctx,
            placement,
            info,
        }
    }

    /*
     * Traverse the data and emit rows/errors via a sender
     */
    async fn collect(
        self: Self,
        sender: Sender<Result<QueryRecord, CustomError>>,
        receiver: Option<Receiver<Result<QueryRecord, CustomError>>>,
    ) {
        log::trace!(
            "Beginning collect for {:#?}\n{:#?}",
            self.placement,
            self.info
        );
        match &self.info.personality {
            NodeType::Nop => (),
            NodeType::Op(op) => self.collect_op(op, sender, receiver.unwrap()).await,
            NodeType::Leaf(leaf) => self.collect_leaf(leaf, sender).await,
        }
    }

    async fn collect_op(
        self: &Self,
        op: &OpType,
        mut sender: Sender<Result<QueryRecord, CustomError>>,
        mut receiver: Receiver<Result<QueryRecord, CustomError>>,
    ) {
        log::trace!("Collecting Op {:?}", op);
        match op {
            OpType::Nop => {}
            OpType::Rename => {}
            OpType::Reorder => loop {
                match receiver.next().await {
                    Some(r) => {
                        log::trace!("OpType::Reorder -> {:?}", r);
                        if let Err(err) = sender.send(r).await {
                            log::error!("Send error while reading data for reorder: {:?}", err);
                            let _ = sender.send(Err(CustomError::from("Send Error")));
                            return;
                        }
                    }
                    None => break,
                }
            },
            OpType::Project => loop {
                match receiver.next().await {
                    Some(r) => {
                        log::trace!("OpType::Project -> {:?}", r);
                        if let Err(err) = sender.send(r).await {
                            log::error!("Send error while reading data for project: {:?}", err);
                            let _ = sender.send(Err(CustomError::from("Send Error")));
                            return;
                        }
                    }
                    None => break,
                }
            },
            OpType::Select => {}
            OpType::Set => {}
            OpType::Join => {}
            OpType::Agg => {}
        }

        ()
    }

    async fn collect_leaf(
        self: &Self,
        leaf: &IoType,
        mut sender: Sender<Result<QueryRecord, CustomError>>,
    ) {
        log::trace!("Collecting Leaf {:?}", leaf);
        match leaf {
            IoType::Ram(table_relation) => {
                let result =
                    tables::TableRelation::find_by_name(self.ctx.user_id, table_relation.clone());
                if let Err(err) = result {
                    log::error!("No table relation found for execution context and graph");
                    let _ = sender.send(Err(err)).await;
                    return;
                }

                let table_cache_map = self.ctx.app_data.table_cache.lock().await;
                let table = result.unwrap();
                log::trace!("Loading table_data from ram cache");
                if let Some(table_data) = table_cache_map.get(&table.id) {
                    log::trace!("Found table_data with {} partitions", table_data.len());
                    for (i, table_partition) in table_data.into_iter().enumerate() {
                        log::trace!(
                            "Processing {} records for partition {}",
                            table_partition.len(),
                            i
                        );
                        for r in table_partition {
                            log::trace!("IoType::Ram -> {:?}", r);
                            if let Err(err) = sender.send(Ok(r.clone())).await {
                                log::error!("Send error while reading data from cache: {:?}", err);
                                let _ = sender.send(Err(CustomError::from("Send Error")));
                                return;
                            }
                        }
                    }
                } else {
                    log::warn!("No data found for table {:?}", table);
                }
            }
            IoType::Disk => {}
            IoType::Network => {}
            IoType::Generator => {}
        }
    }
}

impl HyperNode {
    fn new(name: String, columns: Option<Vec<String>>, info: NodeInfo) -> HyperNode {
        HyperNode {
            name,
            columns,
            info: Arc::new(info),
            execution_info: Arc::new(Mutex::new(None)),
        }
    }
}

#[async_trait]
impl Node for HyperNode {
    fn input(&self) -> NodeInput {
        self.info.input.clone()
    }

    fn personality(&self) -> NodeType {
        self.info.personality.clone()
    }

    async fn curse(
        &self,
        ctx: Arc<ExecuteContext>,
        sender: Sender<Result<QueryRecord, CustomError>>,
    ) -> Result<(), CustomError> {
        // TODO: Look up info to determine how to create WorkNode instances

        // Continue the recursive opening of channels and flow of data
        match self.input() {
            NodeInput::None => (),
            NodeInput::Leaf => {
                // Create a work node and spawn the work to be done by this HyperNode
                let placement = Placement::Server(Partition::Whole); // one shot everything
                let info = self.info.clone(); // work node knows about inputs and partitioning now
                let work_node = WorkNode::new(ctx.clone(), placement, info);

                actix_rt::spawn(WorkNode::collect(work_node, sender, None));
            }
            NodeInput::Single(child) => {
                // Create the channel that produces the input for this HyperNode's single input WorkNodes
                let channel_buf_size = (1 as usize) << 20;
                let (hyper_sender, hyper_receiver) = futures::channel::mpsc::channel::<
                    Result<QueryRecord, CustomError>,
                >(channel_buf_size);

                // Create a work node and spawn the work to be done by this HyperNode
                let placement = Placement::Server(Partition::Whole); // one shot everything
                let info = self.info.clone(); // work node knows about inputs and partitioning now
                let work_node = WorkNode::new(ctx.clone(), placement, info);

                // Spawn a worker to produce data for the sender
                actix_rt::spawn(WorkNode::collect(work_node, sender, Some(hyper_receiver)));

                // Spawn the children to produce data for the worker
                let ctx_clone = ctx.clone();
                actix_rt::spawn(async move {
                    match child.curse(ctx_clone, hyper_sender).await {
                        Ok(()) => (),
                        Err(err) => log::error!("Query Execution Error: {:?}", err),
                    }
                    ()
                });
            }
            NodeInput::Double(left_child, right_child) => {
                // Create the channel that produces the input for this HyperNode's left input WorkNodes
                let channel_buf_size = (1 as usize) << 20;
                let (hyper_sender, left_receiver) = futures::channel::mpsc::channel::<
                    Result<QueryRecord, CustomError>,
                >(channel_buf_size);
                let ctx_clone = ctx.clone();
                actix_rt::spawn(async move {
                    match left_child.curse(ctx_clone, hyper_sender).await {
                        Ok(()) => (),
                        Err(err) => log::error!("Query Execution Error: {:?}", err),
                    }
                    ()
                });

                // Create the channel that produces the input for this HyperNode's right input WorkNodes
                let channel_buf_size = (1 as usize) << 20;
                let (hyper_sender, right_receiver) = futures::channel::mpsc::channel::<
                    Result<QueryRecord, CustomError>,
                >(channel_buf_size);
                let ctx_clone = ctx.clone();
                actix_rt::spawn(async move {
                    match right_child.curse(ctx_clone, hyper_sender).await {
                        Ok(()) => (),
                        Err(err) => log::error!("Query Execution Error: {:?}", err),
                    }
                    ()
                });

                todo!();
            }
        }

        // Nothing else to do here
        Ok(())
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

#[async_trait]
impl Node for RootNode {
    fn input(&self) -> NodeInput {
        match self.graph {
            Some(ref root) => root.input(),
            None => NodeInput::None,
        }
    }

    fn personality(&self) -> NodeType {
        match self.graph {
            Some(ref root) => root.personality(),
            None => NodeType::Nop,
        }
    }

    async fn curse(
        &self,
        ctx: Arc<ExecuteContext>,
        sender: Sender<Result<QueryRecord, CustomError>>,
    ) -> Result<(), CustomError> {
        let mut sender = sender;

        // Create channels to connect each HyperNode, returning the root-level channel receiver
        let root = match self.graph {
            Some(ref n) => n.clone(),
            None => return Err(CustomError::from("Cannot curse from root without graph")),
        };

        // Create the channel that produces the data acutally returned by the root
        let channel_buf_size = (1 as usize) << 20;
        let (root_sender, mut root_receiver) =
            futures::channel::mpsc::channel::<Result<QueryRecord, CustomError>>(channel_buf_size);

        // Begin the recursive opening of channels and flow of data
        actix_rt::spawn(async move {
            match root.curse(ctx, root_sender).await {
                Ok(()) => (),
                Err(err) => log::error!("Query Execution Error: {:?}", err),
            }
            ()
        });

        // Process chunks of data from downstream, moving them upstream or erroring
        loop {
            match root_receiver.next().await {
                Some(r) => match sender.send(r).await {
                    Ok(()) => (),
                    Err(err) => {
                        log::error!("Query Execution Error: {:?}", err);
                        return Err(CustomError::from(format!(
                            "Query Execution Error: {:?}",
                            err
                        )));
                    }
                },
                None => break,
            }
        }

        Ok(())
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
        let select_node = Arc::new(HyperNode::new(
            format!("select_{}", input_relation),
            None,
            NodeInfo {
                input: NodeInput::Leaf,
                personality: NodeType::Leaf(IoType::Ram(input_relation.to_lowercase())),
            },
        ));

        // Second: Project out the data that we want to use
        let project_node = Arc::new(HyperNode::new(
            String::from("project"),
            project_columns.clone(),
            NodeInfo {
                input: NodeInput::Single(select_node),
                personality: NodeType::Op(OpType::Project),
            },
        ));

        // Third: Reorder the data that we want output by the subselect
        let reorder_node = Arc::new(HyperNode::new(
            String::from("reorder"),
            project_columns,
            NodeInfo {
                input: NodeInput::Single(project_node),
                personality: NodeType::Op(OpType::Reorder),
            },
        ));

        // TODO: Nested subselects and joins as recursive nodes in execution graph
        // Hook up the root to the reorder
        let mut root = unsafe { Arc::get_mut_unchecked(&mut self.root) };
        root.graph = Some(reorder_node);

        self
    }

    async fn build(&mut self) -> Arc<RootNode> {
        self.root.clone()
    }
}

pub struct GraphInflator {}

impl GraphInflator {
    pub fn new() -> GraphInflator {
        GraphInflator {}
    }

    pub async fn add_select_stmt(
        &self,
        builder: &mut GraphBuilder,
        select_stmt: SelectStatement,
    ) -> Result<(), CustomError> {
        let columns = None;
        for f in select_stmt.fields.into_iter() {
            match f {
                nom_sql::FieldDefinitionExpression::All => {}
                nom_sql::FieldDefinitionExpression::AllInTable(_) => {
                    return Err(CustomError::from("Unsupported Statement"))
                }
                nom_sql::FieldDefinitionExpression::Value(_) => {
                    return Err(CustomError::from("Unsupported Statement"))
                }
                nom_sql::FieldDefinitionExpression::Col(col) => match col.function {
                    Some(func) => match *func {
                        FunctionExpression::CountStar => {
                            return Err(CustomError::from("Unsupported Statement"))
                        }
                        FunctionExpression::Avg(_, _) => {
                            return Err(CustomError::from("Unsupported Statement"))
                        }
                        FunctionExpression::Count(_, _) => {
                            return Err(CustomError::from("Unsupported Statement"))
                        }
                        FunctionExpression::Sum(_, _) => {
                            return Err(CustomError::from("Unsupported Statement"))
                        }
                        FunctionExpression::Max(_) => {
                            return Err(CustomError::from("Unsupported Statement"))
                        }
                        FunctionExpression::Min(_) => {
                            return Err(CustomError::from("Unsupported Statement"))
                        }
                        FunctionExpression::GroupConcat(_, _) => {
                            return Err(CustomError::from("Unsupported Statement"))
                        }
                    },
                    None => return Err(CustomError::from("Unsupported Statement")),
                },
            }
        }

        let table_name = match select_stmt.tables.len() {
            1 => select_stmt
                .tables
                .first()
                .unwrap()
                .to_string()
                .to_lowercase(),
            _ => return Err(CustomError::from("Unsupported number of tables")),
        };

        builder.add_subselect(columns, table_name);

        Ok(())
    }

    pub async fn inflate(
        &self,
        query_id: i64,
        query: SqlQuery,
    ) -> Result<Arc<RootNode>, CustomError> {
        let mut builder = GraphBuilder::new(query_id);

        match query {
            SqlQuery::Select(stmt) => {
                log::trace!("Found SelectStatement: {:?}", &stmt);
                self.add_select_stmt(&mut builder, stmt).await?;
            }
            _ => {
                log::debug!("Unsupported, valid SqlQuery: {:#?}", query);
                return Err(CustomError::from("Unsupported Statement"));
            }
        }

        let root = builder.build().await;
        Ok(root)
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
            let _ = simple_logger::SimpleLogger::new().init();
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

        let root = GraphInflator::new().inflate(1, sql_query).await.unwrap();
        log::trace!("Inflated execution graph: {:?}", root.as_ref());
    }

    #[actix_rt::test]
    async fn test_can_run_select_star() {
        setup();

        let query = "SELECT * FROM FOO";
        let sql_query = parse_query(query).expect("Failed to parse test query");
        log::trace!("Inflating executing graph for {:?}", sql_query);

        let root = GraphInflator::new().inflate(1, sql_query).await.unwrap();
        log::trace!("Inflated execution graph: {:?}", root.as_ref());

        // TODO
    }
}
