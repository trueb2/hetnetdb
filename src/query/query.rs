use super::sql_types::*;
use crate::{error_handler::CustomError, table_schemas::TableSchema};
use chrono::{DateTime, Utc};
use nom_sql::parser::parse_query;
use nom_sql::SqlQuery;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Query {
    pub id: Option<i64>,
    pub text: String,
    pub parse: Option<SqlQuery>,
    pub optimal_parse: Option<SqlQuery>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordTime {
    pub dt_utc: DateTime<Utc>,
}

impl Default for RecordTime {
    fn default() -> Self {
        RecordTime { dt_utc: Utc::now() }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryRecord {
    pub ready: RecordTime,
    pub columns: Vec<Box<dyn SqlType>>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryResult {
    pub records: Vec<QueryRecord>,
}

impl Query {
    pub fn parse(input_query: &Query) -> Result<Query, CustomError> {
        let mut query = input_query.clone();
        if input_query.parse.is_none() {
            query.parse = Some(parse_query(&input_query.text)?);
            log::info!("Parse: {:#?}", query.parse.as_ref().unwrap());
        } else {
            log::info!("Pre-populated Parse: {:#?}", query.parse.as_ref().unwrap());
        }
        Ok(query)
    }

    pub fn optimize(input_query: &Query) -> Result<Query, CustomError> {
        if input_query.parse.is_none() {
            return Err(CustomError::from("Cannot optimze query without parse"));
        }

        let mut query = input_query.clone();
        query.optimal_parse = query.parse.clone();

        Ok(query)
    }
}

pub struct QueryRecordBuilder {
    into_types: Vec<Box<dyn Fn(String) -> Result<Box<dyn SqlType>, CustomError>>>,
}

impl QueryRecordBuilder {
    pub fn new(table_schema: &TableSchema) -> QueryRecordBuilder {
        let into_types = (&table_schema.column_types)
            .into_iter()
            .map(|type_string| {
                let into_type: Box<dyn Fn(String) -> Result<Box<dyn SqlType>, CustomError>> =
                    match type_string.as_str() {
                        "i64" => Box::new(|s: String| Ok(Box::new(s.parse::<i64>()?))),
                        "f64" => Box::new(|s: String| Ok(Box::new(s.parse::<f64>()?))),
                        "string" => Box::new(|s: String| Ok(Box::new(s))),
                        _ => Box::new(|_s| Ok(Box::new(Null::default()))),
                    };
                into_type
            })
            .collect();

        QueryRecordBuilder {
            into_types: into_types,
        }
    }

    pub fn from_vec(self: &Self, columns: Vec<String>) -> Result<QueryRecord, CustomError> {
        let mut record = QueryRecord {
            ..Default::default()
        };
        let columns: Result<Vec<Box<dyn SqlType>>, _> = (&self.into_types)
            .into_iter()
            .zip(columns.into_iter())
            .map(|item| item.0(item.1))
            .collect();
        record.columns = columns?;
        record.ready = RecordTime::default();
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_rt::test]
    async fn parse_basic_query() {
        let text: String = "SELECT * FROM FOO;".into();
        let query = Query::parse(&Query {
            text,
            ..Default::default()
        });
        query.unwrap();
    }

    #[actix_rt::test]
    async fn parse_less_simple_query() {
        let text: String = "SELECT * FROM BAR GROUP BY BAR.a ORDER BY BAR.a DESC LIMIT 15".into();
        let query = Query::parse(&Query {
            text,
            ..Default::default()
        });
        query.unwrap();
    }

    #[actix_rt::test]
    async fn parse_common_table_expression_simple_query() {
        let text: String = "WITH FOO AS (SELECT * FROM BAR) SELECT * FROM FOO;".into();
        let query = Query::parse(&Query {
            text,
            ..Default::default()
        });
        assert!(query.is_err());
    }

    #[actix_rt::test]
    async fn optimize_less_simple_query() {
        let text: String = "SELECT * FROM BAR GROUP BY BAR.a ORDER BY BAR.a DESC LIMIT 15".into();
        let query = Query::parse(&Query {
            text,
            ..Default::default()
        });
        let query = query.unwrap();
        let query = Query::optimize(&query).unwrap();
        assert_eq!(query.parse, query.optimal_parse);
    }
}
