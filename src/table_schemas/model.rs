use crate::db;
use crate::error_handler::*;
use crate::query::{QueryRecord, QueryRecordBuilder};
use crate::schema::table_schemas;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Queryable, Identifiable, AsChangeset)]
#[table_name = "table_schemas"]
pub struct TableSchema {
    pub id: i64,
    pub column_types: Vec<String>,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Insertable, PartialEq)]
#[table_name = "table_schemas"]
pub struct MaybeTableSchema {
    pub column_types: Vec<String>,
}

impl TableSchema {
    pub fn find_by_id(id: i64) -> Result<TableSchema, CustomError> {
        let conn = db::connection()?;
        let table_schema = table_schemas::table
            .filter(table_schemas::id.eq(id))
            .first(&conn)?;
        Ok(table_schema)
    }

    pub fn find_by_types(maybe_table_schema: MaybeTableSchema) -> Result<TableSchema, CustomError> {
        let conn = db::connection()?;
        let table_schema = table_schemas::table
            .filter(table_schemas::column_types.eq(maybe_table_schema.column_types))
            .first(&conn)?;
        Ok(table_schema)
    }

    pub fn create(maybe_table_schema: MaybeTableSchema) -> Result<TableSchema, CustomError> {
        let conn = db::connection()?;
        let table_schema = diesel::insert_into(table_schemas::table)
            .values(maybe_table_schema)
            .get_result(&conn)?;
        Ok(table_schema)
    }

    pub fn delete(id: i64) -> Result<TableSchema, CustomError> {
        let conn = db::connection()?;
        let table_schema = diesel::delete(table_schemas::table)
            .filter(table_schemas::id.eq(id))
            .get_result(&conn)?;
        Ok(table_schema)
    }

    pub fn verify(self: &Self, raw_data: Vec<u8>) -> Result<Vec<QueryRecord>, CustomError> {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(raw_data.as_slice());
        let query_record_builder = QueryRecordBuilder::new(self);
        let records: Result<Vec<_>, _> = reader
            .deserialize()
            .map(|raw_record| query_record_builder.from_vec(raw_record?))
            .collect();
        let records = records?;
        log::debug!("Verified {} rows for {:?}", records.len(), &self);
        Ok(records)
    }
}

impl From<TableSchema> for MaybeTableSchema {
    fn from(table_schema: TableSchema) -> Self {
        MaybeTableSchema {
            column_types: table_schema.column_types,
        }
    }
}
