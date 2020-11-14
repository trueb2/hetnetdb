use super::query::*;
use super::sql_types::*;
use crate::error_handler::*;
use log;

pub struct Execution {}

impl Execution {
    pub async fn execute(query: Query) -> Result<QueryResult, CustomError> {
        log::debug!(
            "Beginning execution of '{}' with plan {:#?}",
            &query.text,
            &query.optimal_parse
        );

        Ok(QueryResult {
            records: [QueryRecord {
                columns: [42 as i64]
                    .iter()
                    .map(|v| Box::new(*v) as Box<dyn SqlType>)
                    .collect(),
                ..Default::default()
            }]
            .to_vec(),
            ..Default::default()
        })
    }
}
