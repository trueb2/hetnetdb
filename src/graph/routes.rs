use crate::{error_handler::CustomError, AppData};
use actix_web::{get, web, HttpResponse};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct IdRequest {
    id: i64,
}

#[get("/graph/node")]
async fn find_by_id(
    _app_data: web::Data<AppData>,
    web::Query(info): web::Query<IdRequest>,
) -> Result<HttpResponse, CustomError> {
    Ok(HttpResponse::Ok().json(info))
}

pub fn init_routes(config: &mut web::ServiceConfig) {
    config.service(find_by_id);
}
