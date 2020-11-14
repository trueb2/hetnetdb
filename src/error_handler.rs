use actix_web::http::StatusCode;
use actix_web::{HttpResponse, ResponseError};
use diesel::result::Error as DieselError;
use serde::Deserialize;
use serde_json::json;
use std::fmt;
use crypto::symmetriccipher;

#[derive(Debug, Deserialize)]
pub struct CustomError {
    pub error_status_code: u16,
    pub error_message: String,
}

impl CustomError {
    pub fn new(error_status_code: u16, error_message: String) -> CustomError {
        CustomError {
            error_status_code,
            error_message,
        }
    }
}

impl fmt::Display for CustomError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.error_message.as_str())
    }
}

impl From<DieselError> for CustomError {
    fn from(error: DieselError) -> CustomError {
        match error {
            DieselError::DatabaseError(_, err) => CustomError::new(409, err.message().to_string()),
            DieselError::NotFound => {
                CustomError::new(404, "The record is not found".to_string())
            }
            err => CustomError::new(500, format!("Unknown Diesel error: {}", err)),
        }
    }
}

impl From<String> for CustomError {
    fn from(error: String) -> CustomError {
        CustomError { error_message: error, error_status_code: 501 }
    }
}

impl From<std::array::TryFromSliceError> for CustomError {
    fn from(_error: std::array::TryFromSliceError) -> CustomError {
        CustomError { error_message: String::from("Internal server error"), error_status_code: 501 }
    }
}

impl From<symmetriccipher::SymmetricCipherError> for CustomError {
    fn from(_error: symmetriccipher::SymmetricCipherError) -> CustomError {
        CustomError { error_message: String::from("Internal server error"), error_status_code: 501 }
    }
}

impl From<base64::DecodeError> for CustomError {
    fn from(_error: base64::DecodeError) -> CustomError {
        CustomError { error_message: String::from("Internal server error"), error_status_code: 501 }
    }
}

impl From<std::string::FromUtf8Error> for CustomError {
    fn from(_error: std::string::FromUtf8Error) -> CustomError {
        CustomError { error_message: String::from("Internal server error"), error_status_code: 501 }
    }
}

impl From<actix_web::error::Error> for CustomError {
    fn from(_error: actix_web::error::Error) -> CustomError {
        CustomError { error_message: String::from("Bad request"), error_status_code: 400 }
    }
}
impl From<actix_web_httpauth::extractors::AuthenticationError<actix_web_httpauth::headers::www_authenticate::bearer::Bearer>> for CustomError {
    fn from(_error: actix_web_httpauth::extractors::AuthenticationError<actix_web_httpauth::headers::www_authenticate::bearer::Bearer>) -> CustomError {
        CustomError { error_message: String::from("Bad request"), error_status_code: 400 }
    }
}

impl ResponseError for CustomError {
    fn error_response(&self) -> HttpResponse {
        let status_code = match StatusCode::from_u16(self.error_status_code) {
            Ok(status_code) => status_code,
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        let error_message = match status_code.as_u16() < 500 {
            true => self.error_message.clone(),
            false => "Internal server error".to_string(),
        };

        HttpResponse::build(status_code).json(json!({ "message": error_message }))
    }
}
