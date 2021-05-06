use actix_web::HttpRequest;
use jsonwebtoken::{crypto, Algorithm, DecodingKey};

use http::HeaderValue;
use std::fs::File;

use snafu::{ResultExt, Snafu};

/// Authorization Errors
#[derive(Debug, Snafu)]
pub enum AuthError {
    #[snafu(display("Internal error: {}", details))]
    InternalError { details: String },
    #[snafu(display("No Bearer Token was provided in the HTTP Header"))]
    NoBearerToken {},
    #[snafu(display("Invalid token, cannot be parsed into a string: {}", source.to_string()))]
    InvalidTokenStr { source: http::header::ToStrError },
    #[snafu(display("Unauthorized token({}) for uri({})", token, uri))]
    Unauthorized { token: String, uri: String },
    #[snafu(display(
        "Verification process failed, {}. Please check your json web token.",
        source
    ))]
    Verification { source: jsonwebtoken::errors::Error },
    #[snafu(display("Invalid Bearer Token: {}", details))]
    InvalidToken { details: String },
}

/// Initialise JWK with the contents of the file at 'jwk_path'.
/// If jwk_path is 'None', authentication is disabled.
pub fn init(jwk_path: Option<String>) -> JsonWebKey {
    match jwk_path {
        Some(path) => {
            let jwk_file = File::open(path).expect("Failed to open JWK file");
            JsonWebKey::from(Some(jwk_file))
        }
        None => JsonWebKey::from(None),
    }
}

#[derive(serde::Deserialize, Default, Debug)]
pub struct JsonWebKey {
    #[serde(skip_deserializing)]
    enabled: bool,
    #[serde(alias = "alg")]
    algorithm: Algorithm,
    #[serde(alias = "n")]
    modulus: String,
    #[serde(alias = "e")]
    exponent: String,
}

impl JsonWebKey {
    /// Validates and returns new JsonWebKey
    pub(crate) fn from(jwk_file: Option<File>) -> Self {
        match jwk_file {
            Some(jwk_file) => {
                let mut jwk: Self = match serde_json::from_reader(jwk_file) {
                    Ok(jwk) => jwk,
                    Err(e) => panic!("Failed to deserialize the jwk: {}", e),
                };
                jwk.enabled = true;
                jwk
            }
            None => Self::default(),
        }
    }

    /// Validate a bearer token
    pub(crate) fn validate(&self, token: &str, uri: &str) -> Result<(), AuthError> {
        let (message, signature) = split_token(&token)?;
        match crypto::verify(&signature, &message, &self.decoding_key(), self.algorithm()) {
            Ok(true) => Ok(()),
            Ok(false) => Err(AuthError::Unauthorized {
                token: token.to_string(),
                uri: uri.to_string(),
            }),
            Err(source) => Err(AuthError::Verification { source }),
        }
    }

    // Returns true if REST calls should be authenticated.
    fn auth_enabled(&self) -> bool {
        self.enabled
    }

    // Return the algorithm.
    fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    // Return the modulus.
    fn modulus(&self) -> &str {
        &self.modulus
    }

    // Return the exponent.
    fn exponent(&self) -> &str {
        &self.exponent
    }

    // Return the decoding key
    fn decoding_key(&self) -> DecodingKey {
        DecodingKey::from_rsa_components(self.modulus(), self.exponent())
    }
}

/// Authenticate the HTTP request by checking the authorisation token to ensure
/// the sender is who they claim to be.
pub fn authenticate(req: &HttpRequest) -> Result<(), AuthError> {
    let jwk: &JsonWebKey = match req.app_data() {
        Some(jwk) => Ok(jwk),
        None => Err(AuthError::InternalError {
            details: "Json Web Token not configured in the REST server".to_string(),
        }),
    }?;

    // If authentication is disabled there is nothing to do.
    if !jwk.auth_enabled() {
        return Ok(());
    }

    match req.headers().get(http::header::AUTHORIZATION) {
        Some(token) => jwk.validate(&format_token(token)?, &req.uri().to_string()),
        None => Err(AuthError::NoBearerToken {}),
    }
}

// Ensure the token is formatted correctly by removing the "Bearer " prefix if
// present.
fn format_token(token: &HeaderValue) -> Result<String, AuthError> {
    let token = token
        .to_str()
        .context(InvalidTokenStr)?
        .trim_start_matches("Bearer ");
    Ok(token.trim().into())
}

// Split the JSON Web Token (JWT) into 2 parts, message and signature.
// The message comprises the header and payload.
//
// JWT format:
//      <header>.<payload>.<signature>
//      \______  ________/
//             \/
//           message
fn split_token(token: &str) -> Result<(String, String), AuthError> {
    let elems = token.split('.').collect::<Vec<&str>>();
    if elems.len() == 3 {
        let message = format!("{}.{}", elems[0], elems[1]);
        let signature = elems[2];
        Ok((message, signature.into()))
    } else {
        Err(AuthError::InvalidToken {
            details: "Should be formatted as: header.payload.signature".to_string(),
        })
    }
}

#[test]
fn validate_test() {
    let token_file = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("authentication")
        .join("token");
    let mut token = std::fs::read_to_string(token_file).expect("Failed to get bearer token");
    let jwk_file = std::env::current_dir()
        .expect("Failed to get current directory")
        .join("authentication")
        .join("jwk");
    let jwk = init(Some(jwk_file.to_str().unwrap().into()));

    jwk.validate(&token, "uri").expect("Validation should pass");
    // create invalid token
    token.push_str("invalid");
    jwk.validate(&token, "uri")
        .expect_err("Validation should fail with an invalid token");
}
