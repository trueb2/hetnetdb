extern crate diesel;
#[macro_use]
extern crate diesel_migrations;

use actix_service::Service;
use actix_web::middleware::Logger;
use actix_web::{dev::ServiceRequest, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;

use http::header;

use dotenv::dotenv;
use listenfd::ListenFd;
use std::env;

mod auth;
mod db;
mod error_handler;
mod schema;

mod health;

macro_rules! AppFactory {
    () => {
        || {
            App::new()
                .wrap(Logger::default())
                .wrap(HttpAuthentication::bearer(auth::validator))
                .wrap_fn(|req, srv| {
                    let mut req: ServiceRequest = req.into();
                    let headers = req.headers_mut();
                    if !headers.contains_key("authorization") {
                        headers.insert(
                            header::HeaderName::from_static("authorization"),
                            header::HeaderValue::from_static("Bearer _"),
                        )
                    }

                    srv.call(req)
                })
                .configure(health::init_routes)
        }
    };
}

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    env_logger::init();
    db::init();
    auth::init();

    let mut listenfd = ListenFd::from_env();
    let mut server = HttpServer::new(AppFactory!());

    server = match listenfd.take_tcp_listener(0)? {
        Some(listener) => server.listen(listener)?,
        None => {
            let host = env::var("HOST").expect("Please set host in .env");
            let port = env::var("PORT").expect("Please set port in .env");
            server.bind(format!("{}:{}", host, port))?
        }
    };

    server.run().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{test, App};
    use lazy_static::lazy_static;

    lazy_static! {
        static ref FIXTURE: () = {
            dotenv().ok();
            env_logger::init();
            db::init();
            auth::init();
            ()
        };
    }

    pub fn setup() {
        lazy_static::initialize(&FIXTURE);
    }
    #[actix_rt::test]
    async fn test_health_get_without_token() {
        setup();

        let mut app = test::init_service(AppFactory!()()).await;
        let req = test::TestRequest::get().uri("/health").to_request();
        let _resp = test::read_response(&mut app, req).await;
    }
}
