use std::sync::{Arc, RwLock};

use super::query::*;
use crate::{error_handler::*, users::User, AppData, graph};
// use futures::channel::mpsc::{ channel};
use futures_util::StreamExt;
use futures_util::future;
use futures_util::task::Poll;
use graph::ExecuteContext;
use log;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct LineRecord {
    pub line: String,
}

pub struct Execution {}

impl Execution {
    pub async fn execute(
        app_data: Arc<AppData>,
        user: User,
        query: Query,
    ) -> Result<QueryResult, CustomError> {
        log::debug!(
            "Beginning execution of '{}' with plan {:#?}",
            &query.text,
            &query.optimal_parse
        );

        // Verify we have a query to execute
        let sql_query = match query.optimal_parse {
            Some(sql_query) => sql_query,
            None => return Err(CustomError::from("Bad Request. Incomplete query.")),
        };

        // Initialize an execution graph
        let query_id = query.id.unwrap_or_default();
        let root: Arc<dyn graph::Node> = graph::GraphInflator::new()
            .inflate(query_id, sql_query)
            .await?;

        // Create a channel to receive rows processed by the execution graph
        let channel_buf_size = (1 as usize) << 20;
        let (sender, receiver) = futures::channel::mpsc::channel::<Result<QueryRecord, CustomError>>(channel_buf_size);
        let ctx = Arc::new(ExecuteContext { user_id: user.id, app_data });
        root.curse(ctx, sender).await?;

        // Collect all of the records emitted to the channel
        let error = RwLock::new(None);
        let records = receiver
            .map(|r| {
                match r {
                    Ok(_) => (),
                    Err(ref err) => {
                        let mut error = error.write().unwrap();
                        *error = Some(err.clone());
                    }
                }
                r
            })
            .take_until(future::poll_fn(|_ctx| {
                let error = error.read().unwrap();
                return match &*error {
                    Some(_) => Poll::Ready(()),
                    None => Poll::Pending,
                }
            }))
            .map(Result::ok)
            .map(Option::unwrap)
            .collect::<Vec<QueryRecord>>()
            .await;

        // Report errors
        match error.into_inner().unwrap() { // We die
            Some(err) => Err(err.clone()),
            None => {
                // Package up the results and return
                let query_result = QueryResult {
                    records,
                    ..Default::default()
                };
                log::info!("Query {} produced {} records", query_id, query_result.records.len());
                Ok(query_result)
            }
        }
    }
}