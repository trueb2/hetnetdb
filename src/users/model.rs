use crate::db;
use crate::error_handler::CustomError;
use crate::schema::users;
use chrono::NaiveDateTime;
use crypto::bcrypt;
use crypto::buffer::{BufferResult, ReadBuffer, WriteBuffer};
use crypto::{aes, blockmodes, buffer, symmetriccipher};
use diesel::prelude::*;
use lazy_static::lazy_static;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;

/*
 * 1. All endpoints require Bearer Token Authentication
 * 2. All users have a unique token derived from username and token stored in the database
 * 3. No user passwords are ever stored or logged
 * 4. When a user's password changes, previous tokens become invalid
 *
 * How to get a token:
 * 1. Create a user with a password
 * 2. Change a user's password
 *
 * How to change a token:
 * 1. Change the password for a user with username, existing password, and existing token
 *
 * DETAILS
 * Tokens given to users will differ from tokens in db in case leaked
 * 1. In db, username and bcrypted hash string from password
 * 2. In user response, base64 encoded symmetric $-delimited string of
 *        1. auth secret,
 *        2. username,
 *        3. bcrypted hash string of password
 */

lazy_static! {
    pub static ref AUTH_SECRET: String = {
        let secret =
            std::env::var("AUTH_SECRET").expect("AUTH_SECRET required for bearer token validator");
        if secret.as_bytes().len() < 48 {
            panic!("AUTH_SECRET is too short");
        }
        base64::encode(secret)
    };
}

pub fn init() {
    lazy_static::initialize(&AUTH_SECRET);
}

pub fn bcrypt(password: &[u8]) -> Result<Vec<u8>, String> {
    if password.len() > 72 {
        return Err(String::from("Password is too long"));
    }

    // Generate a salt
    let mut salt: [u8; 16] = [0; 16];
    let mut rng = rand::rngs::OsRng::default();
    rng.fill_bytes(&mut salt);

    // Encrypt the password with the salt
    let mut hash: [u8; 24] = [0; 24];
    bcrypt::bcrypt(10, &salt, password, &mut hash);

    let mut result: Vec<u8> = vec![];
    result.extend_from_slice(&salt);
    result.extend_from_slice(&password);
    Ok(result)
}

pub fn symmetric_encrypt(
    data: &[u8],
    key: &[u8],
    iv: &[u8],
) -> Result<Vec<u8>, symmetriccipher::SymmetricCipherError> {
    let mut encryptor =
        aes::cbc_encryptor(aes::KeySize::KeySize256, key, iv, blockmodes::PkcsPadding);

    let mut result = Vec::<u8>::new();
    let mut read_buffer = buffer::RefReadBuffer::new(data);
    let mut buffer = [0; 4096];
    let mut write_buffer = buffer::RefWriteBuffer::new(&mut buffer);

    loop {
        let pass_result = encryptor.encrypt(&mut read_buffer, &mut write_buffer, true)?;
        result.extend(
            write_buffer
                .take_read_buffer()
                .take_remaining()
                .iter()
                .map(|&i| i),
        );
        match pass_result {
            BufferResult::BufferUnderflow => break,
            BufferResult::BufferOverflow => {}
        }
    }

    Ok(result)
}

pub fn symmetric_decrypt(
    encrypted_data: &[u8],
    key: &[u8],
    iv: &[u8],
) -> Result<Vec<u8>, symmetriccipher::SymmetricCipherError> {
    let mut decryptor =
        aes::cbc_decryptor(aes::KeySize::KeySize256, key, iv, blockmodes::PkcsPadding);

    let mut result = Vec::<u8>::new();
    let mut read_buffer = buffer::RefReadBuffer::new(encrypted_data);
    let mut buffer = [0; 4096];
    let mut write_buffer = buffer::RefWriteBuffer::new(&mut buffer);

    loop {
        let pass_result = decryptor.decrypt(&mut read_buffer, &mut write_buffer, true)?;
        result.extend(
            write_buffer
                .take_read_buffer()
                .take_remaining()
                .iter()
                .map(|&i| i),
        );
        match pass_result {
            BufferResult::BufferUnderflow => break,
            BufferResult::BufferOverflow => {}
        }
    }

    Ok(result)
}

#[derive(Debug, Identifiable, Queryable, AsChangeset, Insertable)]
#[table_name = "users"]
pub struct User {
    pub id: i64,
    pub username: String,
    pub password_hash: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthUser {
    pub id: i64,
    pub token: String,
}

#[derive(Serialize, AsChangeset, Insertable)]
#[table_name = "users"]
pub struct InsertableUser {
    pub username: String,
    pub password_hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MaybeUser {
    pub username: String,
    pub password: String,
}

impl User {
    // This is an external token not an internal token, which would just be a bcrypted password
    pub fn find_by_token(token: String) -> Result<Self, CustomError> {
        // Token should be base64 encoded string "username$token"
        let token = base64::decode(token)?;

        let secret = AUTH_SECRET.as_bytes();
        let key: [u8; 32] = secret[0..32].try_into()?;
        let iv: [u8; 16] = secret[32..48].try_into()?;

        let message = symmetric_decrypt(token.as_slice(), &key, &iv)?;
        let message = String::from_utf8(message)?;
        log::trace!("Parsing token: [{}]", message);
        let parts: Vec<&str> = message.split("$").collect();
        if parts.len() != 5 {
            return Err(CustomError {
                error_message: String::from("Unauthorized"),
                error_status_code: 401,
            });
        }
        let username = parts[0];
        let password = parts[1..].join("$");
        log::trace!(
            "Parsed username: [{}] and password: [{}]",
            username,
            password
        );

        let conn = db::connection()?;
        let user = users::table
            .filter(users::username.eq(username))
            .filter(users::password_hash.eq(password))
            .first(&conn)?;
        Ok(user)
    }

    pub fn update(id: i64, user: MaybeUser) -> Result<Self, CustomError> {
        let conn = db::connection()?;
        let password_hash = Self::internal_token(user.username.clone(), user.password)?;
        let insertable_user = InsertableUser {
            username: user.username.clone(),
            password_hash: password_hash,
        };
        let user = diesel::update(users::table)
            .filter(users::id.eq(id))
            .filter(users::username.eq(user.username))
            .set(insertable_user)
            .get_result(&conn)?;
        Ok(user)
    }

    pub fn create(user: MaybeUser) -> Result<Self, CustomError> {
        let user = InsertableUser {
            username: user.username.clone(),
            password_hash: Self::internal_token(user.username, user.password)?,
        };
        let conn = db::connection()?;
        let user = diesel::insert_into(users::table)
            .values(user)
            .get_result(&conn)?;
        Ok(user)
    }

    pub fn delete(id: i64) -> Result<usize, CustomError> {
        let conn = db::connection()?;
        let res = diesel::delete(users::table.filter(users::id.eq(id))).execute(&conn)?;
        Ok(res)
    }

    pub fn count() -> Result<i64, CustomError> {
        let conn = db::connection()?;
        Ok(users::table
            .select(diesel::dsl::count_star())
            .first(&conn)?)
    }

    fn internal_token(username: String, password: String) -> Result<String, CustomError> {
        let db_token = bcrypt(password.as_bytes())?;
        let db_token = base64::encode(db_token.as_slice());
        let db_token = format!("$2$10${}", db_token);
        log::trace!(
            "Prepared username: [{}] and password: [{}]",
            username,
            db_token
        );
        Ok(db_token)
    }
}

impl std::convert::TryInto<AuthUser> for User {
    type Error = CustomError;

    fn try_into(self) -> Result<AuthUser, CustomError> {
        let seed: String = [
            self.username,
            String::from("$"), // Symmetric encryption allows decryption and lookup by username
            self.password_hash, // Token invalidated if password changes
        ]
        .join("");

        // Token invalidated if secret changes
        let secret = AUTH_SECRET.as_bytes();
        let key: [u8; 32] = secret[0..32].try_into()?;
        let iv: [u8; 16] = secret[32..48].try_into()?;

        // Token symmetrically encrypted allows lookup by username
        let token = symmetric_encrypt(seed.as_bytes(), &key, &iv)?;
        let token = base64::encode(token.as_slice());

        Ok(AuthUser {
            id: self.id,
            token: token,
        })
    }
}
