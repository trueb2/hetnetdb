use crate::error_handler::CustomError;
use actix_web::{get, web, HttpResponse};

#[get("/health")]
async fn ping() -> Result<HttpResponse, CustomError> {
    Ok(HttpResponse::Ok().json({}))
}

pub fn init_routes(config: &mut web::ServiceConfig) {
    config.service(ping);
}
