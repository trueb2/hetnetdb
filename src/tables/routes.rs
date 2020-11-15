use super::{InsertableTable, ATable, MaybeTable};
use crate::error_handler::CustomError;
use crate::table_schemas::TableSchema;
use crate::users::User;
use actix_web::{delete, get, put, post, web, HttpResponse};

#[get("/tables/id/{id}")]
async fn find_by_id(user: User, id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::debug!("GET /tables/id/{} (user = {})", id, user.id);
    let table = ATable::find_by_id(user.id, id)?;
    Ok(HttpResponse::Ok().json(table))
}

#[get("/tables/name/{name}")]
async fn find_by_name(user: User, name: web::Path<String>) -> Result<HttpResponse, CustomError> {
    let name = name.into_inner();
    log::debug!("GET /tables/name/{} (user = {})", name, user.id);
    let table = ATable::find_by_name(user.id, name)?;
    Ok(HttpResponse::Ok().json(table))
}

#[post("/tables/upload/{id}")]
async fn upload(user: User, id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::debug!("POST /tables/upload/{} (user = {})", id, user.id);
    todo!();
}

#[put("/tables/{id}")]
async fn update(user: User, id: web::Path<i64>, maybe_table: web::Json<MaybeTable>) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    let maybe_table = maybe_table.into_inner();
    log::debug!("PUT /tables/{} (user = {}) {:?}", id, user.id, maybe_table);
    let table = ATable::update(user.id, id, InsertableTable {
        user_id: user.id,
        table_schema_id: maybe_table.table_schema_id,
        name: maybe_table.name,
        size: 0,
    })?;
    Ok(HttpResponse::Ok().json(table))
}

#[post("/tables")]
async fn create(user: User, maybe_table: web::Json<MaybeTable>) -> Result<HttpResponse, CustomError> {
    let maybe_table = maybe_table.into_inner();
    log::debug!("POST /table (user = {}) {:?}", user.id, maybe_table);

    let table_schema = TableSchema::find_by_id(maybe_table.table_schema_id)?;
    log::trace!("Table {} to use {:?}", maybe_table.name, table_schema);

    let insertable_table = InsertableTable {
        user_id: user.id,
        table_schema_id: table_schema.id,
        name: maybe_table.name,
        size: 0,
    };
    let table = ATable::create(insertable_table)?;
    Ok(HttpResponse::Ok().json(table))
}

#[delete("/tables/{id}")]
async fn delete(user: User, id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::debug!("DELETE /tables/{}", id);
    let table = ATable::delete(user.id, id)?;
    Ok(HttpResponse::Ok().json(table))}

pub fn init_routes(config: &mut web::ServiceConfig) {
    config.service(find_by_id);
    config.service(find_by_name);
    config.service(upload);
    config.service(create);
    config.service(update);
    config.service(delete);
}
