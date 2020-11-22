use super::{MaybeTableSchema, TableSchema};
use crate::error_handler::CustomError;
use actix_web::{delete, get, post, web, HttpResponse};

#[get("/table_schemas/types")]
async fn find_by_types(
    maybe_table_schema: web::Json<MaybeTableSchema>,
) -> Result<HttpResponse, CustomError> {
    let maybe_table_schema = maybe_table_schema.into_inner();
    log::debug!("GET /table_schemas/types {:?}", maybe_table_schema);
    let types: Vec<String> = maybe_table_schema
        .column_types
        .into_iter()
        .map(|s| s.to_lowercase())
        .collect();
    let table_schema = TableSchema::find_by_types(MaybeTableSchema {
        column_types: types,
    })?;
    Ok(HttpResponse::Ok().json(table_schema))
}

#[get("/table_schemas/id/{id}")]
async fn find_by_id(id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::debug!("GET /table_schemas/{}", id);
    let table_schema = TableSchema::find_by_id(id)?;
    Ok(HttpResponse::Ok().json(table_schema))
}

#[post("/table_schemas")]
async fn create(
    maybe_table_schema: web::Json<MaybeTableSchema>,
) -> Result<HttpResponse, CustomError> {
    let mut maybe_table_schema = maybe_table_schema.into_inner();
    log::debug!("POST /table_schemas {:?}", maybe_table_schema);
    maybe_table_schema.column_types = maybe_table_schema.column_types.into_iter().map(|s| s.to_lowercase()).collect();
    let table_schema = TableSchema::create(maybe_table_schema)?;
    Ok(HttpResponse::Ok().json(table_schema))
}

#[delete("/table_schemas/{id}")]
async fn delete(id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::debug!("DELETE /table_schemas/{}", id);
    let table_schema = TableSchema::delete(id)?;
    Ok(HttpResponse::Ok().json(table_schema))
}

pub fn init_routes(config: &mut web::ServiceConfig) {
    config.service(find_by_id);
    config.service(find_by_types);
    config.service(create);
    config.service(delete);
}
