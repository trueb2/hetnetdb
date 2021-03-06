use std::sync::Arc;

use super::execute::Execution;
use super::query::{Query, QueryResult};
use crate::{error_handler::CustomError, users::User, AppData};
use actix_web::{post, web, HttpResponse};

#[post("/query/submit")]
async fn submit(
    app_data: web::Data<AppData>,
    user: User,
    query: web::Json<Query>,
) -> Result<HttpResponse, CustomError> {
    let query = query.into_inner();
    let app_data = app_data.into_inner();
    log::info!("/query/submit {:?}", query);

    // Parse the query
    let query = parse_query(query).await?;

    // Optimize the query
    let query = optimize_query(query).await?;

    // Execute the query
    let results = execute_query(app_data, user, query).await?;

    Ok(HttpResponse::Ok().json(results))
}

async fn parse_query(query: Query) -> Result<Query, CustomError> {
    log::info!("/query/parse {:?}", query);
    let query = Query::parse(&query)?;
    Ok(query)
}

#[post("/query/parse")]
async fn parse(query: web::Json<Query>) -> Result<HttpResponse, CustomError> {
    let query = parse_query(query.into_inner()).await?;
    Ok(HttpResponse::Ok().json(query))
}

async fn optimize_query(query: Query) -> Result<Query, CustomError> {
    log::info!("/query/optimize {:?}", query);
    let query = Query::optimize(&query)?;
    Ok(query)
}

#[post("/query/optimize")]
async fn optimize(query: web::Json<Query>) -> Result<HttpResponse, CustomError> {
    let query = optimize_query(query.into_inner()).await?;
    Ok(HttpResponse::Ok().json(query))
}

async fn execute_query(
    app_data: Arc<AppData>,
    user: User,
    query: Query,
) -> Result<QueryResult, CustomError> {
    log::info!("/query/execute {:?}", query);
    let results = Execution::execute(app_data, user, query).await?;
    Ok(results)
}

#[post("/execute")]
async fn execute(
    app_data: web::Data<AppData>,
    user: User,
    query: web::Json<Query>,
) -> Result<HttpResponse, CustomError> {
    let app_data = app_data.into_inner();
    let result = execute_query(app_data, user, query.into_inner()).await?;
    Ok(HttpResponse::Ok().json(result))
}

pub fn init_routes(config: &mut web::ServiceConfig) {
    config.service(submit);
    config.service(parse);
    config.service(optimize);
    config.service(execute);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_rt::test]
    async fn submit_basic_query() {
        let text: String = "SELECT COUNT(*) FROM FOO;".into();
        serde_json::to_string(&Query {
            text,
            ..Default::default()
        })
        .unwrap();
    }

    #[actix_rt::test]
    async fn submit_query_with_double_quotes() {
        let text: String = "SELECT COUNT(*) FROM FOO WHERE row = \"asdf\";".into();
        serde_json::to_string(&Query {
            text,
            ..Default::default()
        })
        .unwrap();
    }

    #[actix_rt::test]
    async fn submit_query_with_single_quotes() {
        let text: String = "SELECT COUNT(*) FROM FOO WHERE row = 'asdf';".into();
        serde_json::to_string(&Query {
            text,
            ..Default::default()
        })
        .unwrap();
    }
}
