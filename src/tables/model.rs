use crate::db;
use crate::error_handler::*;
use crate::schema::tables;
use crate::table_schemas::TableSchema;
use crate::users::User;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Identifiable, Queryable, Associations)]
#[belongs_to(User)]
#[belongs_to(TableSchema)]
#[table_name = "tables"]
pub struct TableRelation {
    pub id: i64,
    pub user_id: i64,
    pub table_schema_id: i64,
    pub name: String,
    pub size: i64,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize, Insertable, AsChangeset)]
#[table_name = "tables"]
pub struct InsertableTable {
    pub user_id: i64,
    pub table_schema_id: i64,
    pub name: String,
    pub size: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct ComparableTable {
    pub id: i64,
    pub user_id: i64,
    pub table_schema_id: i64,
    pub name: String,
    pub size: i64,
}

#[derive(Debug, Serialize, Deserialize, Insertable, AsChangeset, PartialEq)]
#[table_name = "tables"]
pub struct MaybeTable {
    pub table_schema_id: i64,
    pub name: String,
}

impl TableRelation {
    pub fn find_by_id(user_id: i64, id: i64) -> Result<TableRelation, CustomError> {
        let conn = db::connection()?;
        let table = tables::table
            .filter(tables::id.eq(id))
            .filter(tables::user_id.eq(user_id))
            .first(&conn)?;
        Ok(table)
    }

    pub fn find_by_name(user_id: i64, name: String) -> Result<TableRelation, CustomError> {
        let conn = db::connection()?;
        let table = tables::table
            .filter(tables::name.eq(name))
            .filter(tables::user_id.eq(user_id))
            .first(&conn)?;
        Ok(table)
    }

    pub fn create(insertable_table: InsertableTable) -> Result<TableRelation, CustomError> {
        let conn = db::connection()?;
        let table = diesel::insert_into(tables::table)
            .values(insertable_table)
            .get_result(&conn)?;
        Ok(table)
    }

    pub fn update(
        user_id: i64,
        id: i64,
        insertable_table: InsertableTable,
    ) -> Result<TableRelation, CustomError> {
        let conn = db::connection()?;
        let table = diesel::update(tables::table)
            .filter(tables::id.eq(id))
            .filter(tables::user_id.eq(user_id))
            .set(insertable_table)
            .get_result(&conn)?;
        Ok(table)
    }

    pub fn delete(user_id: i64, id: i64) -> Result<TableRelation, CustomError> {
        let conn = db::connection()?;
        let table = diesel::delete(tables::table)
            .filter(tables::id.eq(id))
            .filter(tables::user_id.eq(user_id))
            .get_result(&conn)?;
        Ok(table)
    }
}

impl From<TableRelation> for MaybeTable {
    fn from(table: TableRelation) -> Self {
        MaybeTable {
            table_schema_id: table.table_schema_id,
            name: table.name,
        }
    }
}

impl From<TableRelation> for ComparableTable{
    fn from(table: TableRelation) -> Self {
        ComparableTable {
            id: table.id,
            user_id: table.user_id,
            table_schema_id: table.table_schema_id,
            name: table.name,
            size: table.size,
        }
    }
}
