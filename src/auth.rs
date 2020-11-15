use crate::users;
use actix_web::{dev::ServiceRequest, Error};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use std::convert::TryInto;

pub fn init() {
    users::init();

    // Bootstrap auth with an admin user if necessary
    let num_users = users::User::count().unwrap();
    if num_users == 0 {
        log::warn!("Bootstrapping auth by creating admin:admin user");
        log::warn!("Remember to update the username and password of admin:admin user");
        let user = users::User::create(users::MaybeUser {
            username: String::from("admin"),
            password: String::from("admin"),
        })
        .unwrap();
        let auth_user: users::AuthUser = user.try_into().unwrap();
        log::warn!(
            "The initial token for admin:admin with id {} is 'Bearer {}'",
            auth_user.id,
            auth_user.token
        );
    }
}

pub async fn validator(
    req: ServiceRequest,
    _credentials: BearerAuth,
) -> Result<ServiceRequest, Error> {
    Ok(req)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_rt::test]
    async fn can_validate_user_token() {
        let user = users::User::create(users::MaybeUser {
            username: "foo".into(),
            password: "bar".into(),
        })
        .unwrap();
        let auth_user: users::AuthUser = user.try_into().unwrap();
        let bearer_token = auth_user.token;
        let _lookup = users::User::find_by_token(bearer_token).unwrap();
    }
}
