use actix_web_httpauth::extractors::bearer::BearerAuth;
use actix_web::{Error, dev::ServiceRequest};


pub fn init() { }

pub async fn validator(req: ServiceRequest, _credentials: BearerAuth) -> Result<ServiceRequest, Error> {
    Ok(req)
}