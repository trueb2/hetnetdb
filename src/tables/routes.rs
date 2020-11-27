use super::{InsertableTable, MaybeTable, TableRelation};
use crate::table_schemas::TableSchema;
use crate::users::User;
use crate::{error_handler::CustomError, query::QueryRecord, AppData};
use actix_multipart::Multipart;
use actix_web::{delete, get, post, put, web, HttpResponse};
use futures::{io::Cursor, StreamExt, TryStreamExt};
use futures_util::AsyncWriteExt;

#[get("/tables/id/{id}")]
async fn find_by_id(user: User, id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::debug!("GET /tables/id/{} (user = {})", id, user.id);
    let table = TableRelation::find_by_id(user.id, id)?;
    Ok(HttpResponse::Ok().json(table))
}

#[get("/tables/name/{name}")]
async fn find_by_name(user: User, name: web::Path<String>) -> Result<HttpResponse, CustomError> {
    let name = name.into_inner();
    log::debug!("GET /tables/name/{} (user = {})", name, user.id);
    let table = TableRelation::find_by_name(user.id, name)?;
    Ok(HttpResponse::Ok().json(table))
}

#[post("/tables/upload/{id}")]
async fn upload(
    app_data: web::Data<AppData>,
    user: User,
    id: web::Path<i64>,
    mut uploaded_data: Multipart,
) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::debug!("POST /tables/upload/{} (user = {})", id, user.id);

    let user_id = user.id;
    let table = web::block(move || TableRelation::find_by_id(user_id, id)).await?;
    let file_dir = format!(
        "/tmp/tables/upload/{}/{}",
        user.id,
        sanitize_filename::sanitize(&table.name)
    );
    let file_dir_clone = file_dir.clone();
    let _result = web::block(move || {
        log::trace!("Creating {} if not exists", file_dir_clone);
        std::fs::create_dir_all(file_dir_clone)
    })
    .await?;

    let mut file_size: i64 = 0;
    let mut data_buffer = Cursor::new(Vec::with_capacity(1024));
    while let Ok(Some(mut field)) = uploaded_data.try_next().await {
        let content_disposition = field.content_disposition().unwrap();
        let filename = content_disposition.get_filename().unwrap();
        let filepath = format!("{}/{}", &file_dir, sanitize_filename::sanitize(&filename));
        log::trace!("Buffering /tables/upload/{} at {}", id, filepath);

        // Field in turn is stream of *Bytes* object
        while let Some(chunk) = field.next().await {
            let chunk = chunk?;
            file_size += chunk.len() as i64;
            data_buffer.write_all(chunk.as_ref()).await?;
        }
    }
    log::debug!("Uploaded {} to {}", file_size, file_dir);

    // Verify the data is valid for the selected schema
    let uploaded_data = data_buffer.into_inner();
    let table_schema_id = table.table_schema_id;
    let table_schema = web::block(move || TableSchema::find_by_id(table_schema_id)).await?;
    let uploaded_records: Vec<QueryRecord> = table_schema.verify(uploaded_data)?;

    // Extend the Data Cache
    {
        let mut table_cache_map = app_data.table_cache.lock().await;
        if table_cache_map.contains_key(&table.id) {
            table_cache_map
                .get_mut(&table.id)
                .unwrap()
                .push(uploaded_records);
        } else {
            table_cache_map.insert(table.id.clone(), vec![uploaded_records]);
        }
    }

    // Update the table info
    let insertable_table = InsertableTable {
        user_id: user.id,
        table_schema_id: table.table_schema_id,
        name: table.name,
        size: file_size,
    };
    let table = web::block(move || TableRelation::update(user.id, id, insertable_table)).await?;

    Ok(HttpResponse::Ok().json(table))
}

#[put("/tables/{id}")]
async fn update(
    user: User,
    id: web::Path<i64>,
    maybe_table: web::Json<MaybeTable>,
) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    let maybe_table = maybe_table.into_inner();
    log::debug!("PUT /tables/{} (user = {}) {:?}", id, user.id, maybe_table);
    let table = TableRelation::update(
        user.id,
        id,
        InsertableTable {
            user_id: user.id,
            table_schema_id: maybe_table.table_schema_id,
            name: maybe_table.name,
            size: 0,
        },
    )?;
    Ok(HttpResponse::Ok().json(table))
}

#[post("/tables")]
async fn create(
    user: User,
    maybe_table: web::Json<MaybeTable>,
) -> Result<HttpResponse, CustomError> {
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
    let table = TableRelation::create(insertable_table)?;
    Ok(HttpResponse::Ok().json(table))
}

#[delete("/tables/{id}")]
async fn delete(user: User, id: web::Path<i64>) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::debug!("DELETE /tables/{}", id);
    let table = TableRelation::delete(user.id, id)?;
    Ok(HttpResponse::Ok().json(table))
}

pub fn init_routes(config: &mut web::ServiceConfig) {
    config.service(find_by_id);
    config.service(find_by_name);
    config.service(upload);
    config.service(create);
    config.service(update);
    config.service(delete);
}
