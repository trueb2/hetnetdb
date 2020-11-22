use std::sync::Arc;

use super::query::*;
use super::sql_types::*;
use crate::{AppData, users::User, error_handler::*, tables};
use log;
use nom_sql::{FunctionExpression, SqlQuery};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct LineRecord {
    pub line: String,
}

pub struct Execution {}

impl Execution {
    pub async fn execute(app_data: Arc<AppData>, user: User, query: Query) -> Result<QueryResult, CustomError> {
        log::debug!(
            "Beginning execution of '{}' with plan {:#?}",
            &query.text,
            &query.optimal_parse
        );

        let sql_query = match query.optimal_parse {
            Some(sql_query) => sql_query,
            None => return Err(CustomError::from("Bad Request. Incomplete query."))
        };

        let select_stmt = match sql_query {
            SqlQuery::Select(select_stmt) => {
                log::trace!("Found SelectStatement: {:#?}", &select_stmt);
                select_stmt
            },
            _ => {
                log::debug!("Unsupported, valid SqlQuery: {:#?}", sql_query);
                return Err(CustomError::from("Unsupported Statement"))
            }
        };

        for f in select_stmt.fields.into_iter() {
            match f {
                nom_sql::FieldDefinitionExpression::All => return Err(CustomError::from("Unsupported Statement")),
                nom_sql::FieldDefinitionExpression::AllInTable(_) => return Err(CustomError::from("Unsupported Statement")),
                nom_sql::FieldDefinitionExpression::Value(_) => return Err(CustomError::from("Unsupported Statement")),
                nom_sql::FieldDefinitionExpression::Col(col) => {
                    match col.function {
                        Some(func) => {
                            match *func {
                                FunctionExpression::CountStar => {},
                                FunctionExpression::Avg(_, _) => return Err(CustomError::from("Unsupported Statement")),
                                FunctionExpression::Count(_, _) => return Err(CustomError::from("Unsupported Statement")),
                                FunctionExpression::Sum(_, _) => return Err(CustomError::from("Unsupported Statement")),
                                FunctionExpression::Max(_) => return Err(CustomError::from("Unsupported Statement")),
                                FunctionExpression::Min(_) => return Err(CustomError::from("Unsupported Statement")),
                                FunctionExpression::GroupConcat(_, _) => return Err(CustomError::from("Unsupported Statement")),
                            }
                        }
                        None => return Err(CustomError::from("Unsupported Statement"))
                    }
                }
            }
        }

        let table_name = match select_stmt.tables.len() {
            1 => {
                select_stmt.tables.first().unwrap().to_string().to_lowercase()
            },
            _ => return Err(CustomError::from("Unsupported number of tables"))
        };

        let table = tables::TableRelation::find_by_name(user.id, table_name)?;
        log::debug!("Sourcing data from {:?}", table);

        let table_cache_map = app_data
            .table_cache
            .lock()
            .await;

        let mut count = 0 as i64;
        if let Some(table_data) = table_cache_map.get(&table.id) {
            log::trace!("Found table_data with {} partitions", table_data.len());
            for table_partition in table_data.into_iter() {
                let length = table_partition.len();
                log::trace!("Adding table_data partition of length {}", length);
                count += length as i64;
            }
        } else {
            log::warn!("No data found for table {:?}", table);
        }

        Ok(QueryResult {
            records: [QueryRecord {
                columns: [count as i64]
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
