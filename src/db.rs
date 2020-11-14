use crate::error_handler::CustomError;
use diesel::Connection;
use diesel::pg::PgConnection;
use diesel::r2d2::ConnectionManager;
use lazy_static::lazy_static;
use r2d2;
use std::env;

type Pool = r2d2::Pool<ConnectionManager<PgConnection>>;
pub type DbConnection = r2d2::PooledConnection<ConnectionManager<PgConnection>>;

embed_migrations!();

lazy_static! {
    static ref POOL: Pool = {
        let (db_url, pool_size) = match cfg!(test) {
            true => (String::from("postgres://postgres:postgres@localhost/asset_api"), 1),
            false => (env::var("DATABASE_URL").expect("Database url not set"), 10)
        };

        let manager = ConnectionManager::<PgConnection>::new(db_url);
        let pool = r2d2::Builder::new().max_size(pool_size).build(manager).expect("Failed to create db pool");
        if cfg!(test) {
            let conn = pool.get().expect("Failed to get db connection from pool");
            conn.begin_test_transaction().expect("Failed to start test transaction")
        }
        pool
    };
}

pub fn init() {
    lazy_static::initialize(&POOL);
    let conn = connection().expect("Failed to get db connection");
    embedded_migrations::run(&conn).unwrap();
}

pub fn connection() -> Result<DbConnection, CustomError> {
    POOL.get()
        .map_err(|e| CustomError::new(500, format!("Failed getting db connection: {}", e)))
}
