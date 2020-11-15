use crate::schema::tables;
use crate::table_schemas::TableSchema;
use crate::users::User;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Identifiable, Queryable, Associations)]
#[belongs_to(User)]
#[belongs_to(TableSchema)]
#[table_name = "tables"]
pub struct Table {
    pub id: i64,
    pub user_id: i64,
    pub table_schema_id: i64,
    pub filename: String,
    pub size: i64,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MaybeTable {
    pub filename: String,
}
