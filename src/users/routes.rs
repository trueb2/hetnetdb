use crate::error_handler::CustomError;
use crate::users::{AuthUser, MaybeUser, User};
use actix_web::{dev::Payload, post, put, web, FromRequest, HttpRequest, HttpResponse};
use actix_web_httpauth::extractors::bearer::BearerAuth;
use futures::executor::block_on;
use futures_util::future::{err, ok, Ready};
use log;
use std::convert::TryInto;

impl FromRequest for User {
    type Error = CustomError;
    type Future = Ready<Result<Self, Self::Error>>;
    type Config = ();

    fn from_request(req: &HttpRequest, payload: &mut Payload) -> Self::Future {
        let bearer_auth = block_on(BearerAuth::from_request(req, payload));
        let bearer_auth = match bearer_auth {
            Ok(auth) => auth,
            Err(error) => return err(Self::Error::from(error)),
        };
        let user = match User::find_by_token(String::from(bearer_auth.token())) {
            Ok(user) => user,
            Err(error) => match error.error_status_code {
                404 => return err(CustomError::new(401, String::from("Unauthorized"))),
                _ => return err(Self::Error::from(error)),
            },
        };
        ok(user)
    }
}

#[put("/users/{id}")]
async fn update(
    user: User,
    id: web::Path<i64>,
    maybe_user: web::Json<MaybeUser>,
) -> Result<HttpResponse, CustomError> {
    let id = id.into_inner();
    log::trace!("PUT /users/{}", id);
    if user.id != id {
        return Err(CustomError::new(401, String::from("Unauthorized")));
    }
    let maybe_user = maybe_user.into_inner();
    let user = User::update(id, maybe_user)?;
    let auth_user: AuthUser = user.try_into()?;
    Ok(HttpResponse::Ok().json(auth_user))
}

#[post("/users")]
async fn create(user: web::Json<MaybeUser>) -> Result<HttpResponse, CustomError> {
    let user = user.into_inner();
    log::trace!("POST /users");
    let user = User::create(user)?;
    let auth_user: AuthUser = user.try_into()?;
    Ok(HttpResponse::Ok().json(auth_user))
}

pub fn init_routes(comfig: &mut web::ServiceConfig) {
    comfig.service(update);
    comfig.service(create);
}
