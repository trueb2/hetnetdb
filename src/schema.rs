table! {
    table_schemas (id) {
        id -> Int8,
        column_types -> Array<Text>,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

table! {
    tables (id) {
        id -> Int8,
        user_id -> Int8,
        table_schema_id -> Int8,
        name -> Text,
        size -> Int8,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

table! {
    users (id) {
        id -> Int8,
        username -> Varchar,
        password_hash -> Varchar,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

joinable!(tables -> table_schemas (table_schema_id));
joinable!(tables -> users (user_id));

allow_tables_to_appear_in_same_query!(
    table_schemas,
    tables,
    users,
);
