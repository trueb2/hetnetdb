use super::MaybeTable;
use crate::error_handler::CustomError;
use actix_web::{delete, get, post, web, HttpResponse};

#[get("/tables/{id}")]
async fn find_by_id(_id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    todo!();
}

#[post("/tables/upload/{id}")]
async fn upload(_id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    todo!();
}

#[post("/tables/{table_schema_id}")]
async fn create(
    _table_schema_id: web::Path<i64>,
    _maybe_table: web::Json<MaybeTable>,
) -> Result<HttpResponse, CustomError> {
    todo!();
}

#[delete("/tables/{id}")]
async fn delete(_id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    todo!();
}

pub fn init_routes(config: &mut web::ServiceConfig) {
    config.service(find_by_id);
    config.service(upload);
    config.service(create);
    config.service(delete);
}
