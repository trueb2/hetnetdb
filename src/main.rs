#![feature(get_mut_unchecked)]
#![feature(async_closure)]

#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_migrations;

use actix_service::Service;
use actix_web::middleware::Logger;
use actix_web::{dev::ServiceRequest, web, App, HttpServer};
use actix_web_httpauth::middleware::HttpAuthentication;

use futures::lock::Mutex;
use http::header;

use dotenv::dotenv;
use listenfd::ListenFd;
use std::{collections::HashMap, env};

mod auth;
mod db;
mod error_handler;
mod schema;

mod graph;
mod health;
mod query;
mod table_schemas;
mod tables;
mod users;

pub struct AppData {
    pub table_cache: Mutex<HashMap<i64, Vec<Vec<query::QueryRecord>>>>,
}

macro_rules! AppFactory {
    ($shared_app_data:expr) => {
        move || {
            App::new()
                .app_data($shared_app_data)
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
                .configure(query::init_routes)
                .configure(users::init_routes)
                .configure(tables::init_routes)
                .configure(table_schemas::init_routes)
                .configure(graph::init_routes)
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

    let app_data = web::Data::new(AppData {
        table_cache: Mutex::new(HashMap::new()),
    });
    let mut server = HttpServer::new(AppFactory!(app_data.clone()));

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
    use actix_web::{http::StatusCode, test, web::Bytes, App};
    use lazy_static::lazy_static;
    use std::convert::TryInto;

    lazy_static! {
        static ref APP_DATA: web::Data<AppData> = {
            web::Data::new(AppData {
                table_cache: Mutex::new(HashMap::new()),
            })
        };
        static ref FIXTURE: () = {
            dotenv().ok();
            let _ = simple_logger::SimpleLogger::new().init();
            db::init();
            auth::init();
            ()
        };
        static ref ADMIN_USER: users::AuthUser = {
            let user = users::User::create(users::MaybeUser {
                username: "hnadmin".into(),
                password: "hnadmin".into(),
            })
            .expect("Failed to create test admin user");
            user.try_into().expect("Failed to create auth user")
        };
    }

    pub fn setup() {
        lazy_static::initialize(&APP_DATA);
        lazy_static::initialize(&FIXTURE);
        lazy_static::initialize(&ADMIN_USER);
    }

    #[actix_rt::test]
    async fn test_health_get_without_token() {
        setup();

        let mut app = test::init_service(AppFactory!(APP_DATA.clone())()).await;
        let req = test::TestRequest::get().uri("/health").to_request();
        let _resp = test::read_response(&mut app, req).await;
    }

    #[actix_rt::test]
    async fn test_create_and_use_user() {
        setup();

        let mut app = test::init_service(AppFactory!(APP_DATA.clone())()).await;

        let user = users::MaybeUser {
            username: String::from("foo"),
            password: String::from("secretpassword"),
        };
        let payload = serde_json::to_string(&user).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/users")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload)
            .to_request();
        let resp: users::AuthUser = test::read_response_json(&mut app, req).await;
        log::debug!("Created User: {:?}", resp);

        let req = test::TestRequest::get()
            .uri("/auth")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", resp.token).as_str(),
            )
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_rt::test]
    async fn test_user_cant_change_other_users() {
        setup();

        let mut app = test::init_service(AppFactory!(APP_DATA.clone())()).await;

        // Create user1
        let maybe_user = users::MaybeUser {
            username: String::from("user1"),
            password: String::from("secretpassword"),
        };
        let payload = serde_json::to_string(&maybe_user).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/users")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload)
            .to_request();
        let user1: users::AuthUser = test::read_response_json(&mut app, req).await;
        log::debug!("Created User: {:?}", user1);

        // Change user1's password as user1
        let maybe_user = users::MaybeUser {
            username: String::from("user1"),
            password: String::from("newsecretpassword"),
        };
        let payload = serde_json::to_string(&maybe_user).expect("Invalid value");

        let req = test::TestRequest::put()
            .uri(format!("/users/{}", user1.id).as_str())
            .header(header::AUTHORIZATION, format!("Bearer {}", user1.token))
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload)
            .to_request();
        let user1: users::AuthUser = test::read_response_json(&mut app, req).await;
        log::debug!("Updated User: {:?}", user1);

        // Use user1's new token
        let req = test::TestRequest::get()
            .uri("/auth")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", user1.token).as_str(),
            )
            .to_request();
        let protected_resp = test::call_service(&mut app, req).await;
        assert_eq!(protected_resp.status(), StatusCode::OK);

        // Create user2
        let maybe_user = users::MaybeUser {
            username: String::from("user2"),
            password: String::from("secretpassword"),
        };
        let payload = serde_json::to_string(&maybe_user).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/users")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload)
            .to_request();
        let user2: users::AuthUser = test::read_response_json(&mut app, req).await;
        log::debug!("Created User: {:?}", user2);

        // Fail to change user1's password
        let payload = serde_json::to_string(&maybe_user).expect("Invalid value");

        let req = test::TestRequest::put()
            .uri(format!("/users/{}", user1.id).as_str())
            .header(header::AUTHORIZATION, format!("Bearer {}", user2.token))
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload)
            .to_request();
        let protected_resp = test::call_service(&mut app, req).await;
        assert_eq!(protected_resp.status(), StatusCode::UNAUTHORIZED);

        // Use user1's token
        let req = test::TestRequest::get()
            .uri("/auth")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", user1.token).as_str(),
            )
            .to_request();
        let protected_resp = test::call_service(&mut app, req).await;
        assert_eq!(protected_resp.status(), StatusCode::OK);

        // Use user2's token
        let req = test::TestRequest::get()
            .uri("/auth")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", user2.token).as_str(),
            )
            .to_request();
        let protected_resp = test::call_service(&mut app, req).await;
        assert_eq!(protected_resp.status(), StatusCode::OK);
    }

    #[actix_rt::test]
    async fn test_create_and_find_table_schemas() {
        setup();

        let mut app = test::init_service(AppFactory!(APP_DATA.clone())()).await;

        let table_schema = table_schemas::MaybeTableSchema {
            column_types: ["string", "i64", "f64"]
                .iter()
                .map(|s| String::from(*s))
                .collect(),
        };
        let payload = serde_json::to_string(&table_schema).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/table_schemas")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let resp: table_schemas::TableSchema = test::read_response_json(&mut app, req).await;

        let req = test::TestRequest::get()
            .uri(format!("/table_schemas/id/{}", resp.id).as_str())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .to_request();
        let resp1: table_schemas::TableSchema = test::read_response_json(&mut app, req).await;
        assert_eq!(table_schema, table_schemas::MaybeTableSchema::from(resp1));

        let req = test::TestRequest::get()
            .uri("/table_schemas/types")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let resp2: table_schemas::TableSchema = test::read_response_json(&mut app, req).await;
        assert_eq!(table_schema, table_schemas::MaybeTableSchema::from(resp2));

        let req = test::TestRequest::delete()
            .uri(format!("/table_schemas/{}", resp.id).as_str())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[actix_rt::test]
    async fn test_use_tables() {
        setup();

        let mut app = test::init_service(AppFactory!(APP_DATA.clone())()).await;

        let table_schema = table_schemas::MaybeTableSchema {
            column_types: ["string", "i64", "f64"]
                .iter()
                .map(|s| String::from(*s))
                .collect(),
        };
        let payload = serde_json::to_string(&table_schema).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/table_schemas")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let table_schema: table_schemas::TableSchema =
            test::read_response_json(&mut app, req).await;

        let maybe_table = tables::MaybeTable {
            table_schema_id: table_schema.id,
            name: "first_table".into(),
        };
        let payload = serde_json::to_string(&maybe_table).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/tables")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let resp: tables::TableRelation = test::read_response_json(&mut app, req).await;
        assert_eq!(maybe_table, tables::MaybeTable::from(resp.clone()));

        let maybe_table = tables::MaybeTable {
            table_schema_id: table_schema.id,
            name: "renamed_first_table".into(),
        };
        let payload = serde_json::to_string(&maybe_table).expect("Invalid value");

        let req = test::TestRequest::put()
            .uri(format!("/tables/{}", resp.id).as_str())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let resp: tables::TableRelation = test::read_response_json(&mut app, req).await;
        assert_eq!(maybe_table, tables::MaybeTable::from(resp.clone()));

        let req = test::TestRequest::delete()
            .uri(format!("/tables/{}", resp.id).as_str())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .to_request();
        let resp: tables::TableRelation = test::read_response_json(&mut app, req).await;
        assert_eq!(maybe_table, tables::MaybeTable::from(resp));
    }

    #[actix_rt::test]
    async fn test_multipart_upload_works() {
        setup();
        let mut app = test::init_service(AppFactory!(APP_DATA.clone())()).await;
        let table_name = "test_multipart_upload_works";

        // We are going to upload this data
        let content_type = "multipart/form-data; boundary=0150c250cceb4434b3ea2f7ed7e87dfc";
        let multipart_payload = Bytes::from(
            "\r\n\
             --0150c250cceb4434b3ea2f7ed7e87dfc\r\n\
             Content-Disposition: form-data; name=\"csv\"; filename=\"sequence.csv\"\r\n\
             Content-Type: text/csv\r\n\r\n\
             1\n\
             2\n\
             3\n\
             4\n\
             5\n\
             6\n\
             7\n\
             8\n\
             9\n\
             10\n\
             \r\n--0150c250cceb4434b3ea2f7ed7e87dfc--\r\n",
        );

        let table_schema = table_schemas::MaybeTableSchema {
            column_types: ["i64"].iter().map(|s| String::from(*s)).collect(),
        };
        let payload = serde_json::to_string(&table_schema).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/table_schemas")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let table_schema: table_schemas::TableSchema =
            test::read_response_json(&mut app, req).await;

        let maybe_table = tables::MaybeTable {
            table_schema_id: table_schema.id,
            name: table_name.into(),
        };
        let payload = serde_json::to_string(&maybe_table).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/tables")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let table: tables::TableRelation = test::read_response_json(&mut app, req).await;
        assert_eq!(maybe_table, tables::MaybeTable::from(table.clone()));

        let req = test::TestRequest::post()
            .uri(format!("/tables/upload/{}", table.id).as_str())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, content_type)
            .set_payload(multipart_payload)
            .to_request();
        let table_after_upload: tables::TableRelation =
            test::read_response_json(&mut app, req).await;
        let mut expected_table = table.clone();
        expected_table.size = 21;
        assert_eq!(
            tables::ComparableTable::from(table_after_upload),
            tables::ComparableTable::from(expected_table)
        );
    }

    // #[actix_rt::test]
    async fn _test_count_star() {
        setup();
        let mut app = test::init_service(AppFactory!(APP_DATA.clone())()).await;
        let table_name = "test_count_star";

        // We are going to upload this data
        let content_type = "multipart/form-data; boundary=0150c250cceb4434b3ea2f7ed7e87dfc";
        let multipart_payload = Bytes::from(
            "\r\n\
             --0150c250cceb4434b3ea2f7ed7e87dfc\r\n\
             Content-Disposition: form-data; name=\"csv\"; filename=\"sequence.csv\"\r\n\
             Content-Type: text/csv\r\n\r\n\
             1\n\
             2\n\
             3\n\
             4\n\
             5\n\
             6\n\
             7\n\
             8\n\
             9\n\
             10\n\
             \r\n--0150c250cceb4434b3ea2f7ed7e87dfc--\r\n",
        );

        let table_schema = table_schemas::MaybeTableSchema {
            column_types: ["i64"].iter().map(|s| String::from(*s)).collect(),
        };
        let payload = serde_json::to_string(&table_schema).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/table_schemas")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let table_schema: table_schemas::TableSchema =
            test::read_response_json(&mut app, req).await;

        let maybe_table = tables::MaybeTable {
            table_schema_id: table_schema.id,
            name: table_name.into(),
        };
        let payload = serde_json::to_string(&maybe_table).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/tables")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let table: tables::TableRelation = test::read_response_json(&mut app, req).await;
        assert_eq!(maybe_table, tables::MaybeTable::from(table.clone()));

        let req = test::TestRequest::post()
            .uri(format!("/tables/upload/{}", table.id).as_str())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, content_type)
            .set_payload(multipart_payload)
            .to_request();
        let table_after_upload: tables::TableRelation =
            test::read_response_json(&mut app, req).await;
        let mut expected_table = table.clone();
        expected_table.size = 21;
        assert_eq!(
            tables::ComparableTable::from(table_after_upload),
            tables::ComparableTable::from(expected_table)
        );
        let req = test::TestRequest::post()
            .uri("/query/submit")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload("{\"text\": \"select count(*) from test_count_star\"}")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // let result: query::QueryResult = test::read_response_json(&mut app, req).await;
        // assert_eq!(result.records[0].columns[0]["i64"], 20);
    }

       #[actix_rt::test]
    async fn _test_select_star() {
        setup();
        let mut app = test::init_service(AppFactory!(APP_DATA.clone())()).await;
        let table_name = "test_select_star";

        // We are going to upload this data
        let content_type = "multipart/form-data; boundary=0150c250cceb4434b3ea2f7ed7e87dfc";
        let multipart_payload = Bytes::from(
            "\r\n\
             --0150c250cceb4434b3ea2f7ed7e87dfc\r\n\
             Content-Disposition: form-data; name=\"csv\"; filename=\"sequence.csv\"\r\n\
             Content-Type: text/csv\r\n\r\n\
             1\n\
             2\n\
             3\n\
             4\n\
             5\n\
             6\n\
             7\n\
             8\n\
             9\n\
             10\n\
             \r\n--0150c250cceb4434b3ea2f7ed7e87dfc--\r\n",
        );

        let table_schema = table_schemas::MaybeTableSchema {
            column_types: ["i64"].iter().map(|s| String::from(*s)).collect(),
        };
        let payload = serde_json::to_string(&table_schema).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/table_schemas")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let table_schema: table_schemas::TableSchema =
            test::read_response_json(&mut app, req).await;

        let maybe_table = tables::MaybeTable {
            table_schema_id: table_schema.id,
            name: table_name.into(),
        };
        let payload = serde_json::to_string(&maybe_table).expect("Invalid value");

        let req = test::TestRequest::post()
            .uri("/tables")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload.clone())
            .to_request();
        let table: tables::TableRelation = test::read_response_json(&mut app, req).await;
        assert_eq!(maybe_table, tables::MaybeTable::from(table.clone()));

        let req = test::TestRequest::post()
            .uri(format!("/tables/upload/{}", table.id).as_str())
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, content_type)
            .set_payload(multipart_payload)
            .to_request();
        let table_after_upload: tables::TableRelation =
            test::read_response_json(&mut app, req).await;
        let mut expected_table = table.clone();
        expected_table.size = 21;
        assert_eq!(
            tables::ComparableTable::from(table_after_upload),
            tables::ComparableTable::from(expected_table)
        );
        let req = test::TestRequest::post()
            .uri("/query/submit")
            .header(
                header::AUTHORIZATION,
                format!("Bearer {}", ADMIN_USER.token),
            )
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload("{\"text\": \"select * from test_select_star\"}")
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // let result: query::QueryResult = test::read_response_json(&mut app, req).await;
        // assert_eq!(result.records[0].columns[0]["i64"], 20);
    }
}
