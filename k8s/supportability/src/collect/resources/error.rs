/// ResourceError is a wrapper around possible errors that can occur while interacting
/// interacting or processing results obtained from REST service
#[derive(Debug)]
#[allow(clippy::enum_variant_names)]
pub enum ResourceError {
    IOError(std::io::Error),
    RestJsonError(openapi::clients::tower::Error<openapi::models::RestJsonError>),
    JSONError(serde_json::Error),
    UUIDParseError(uuid::Error),
    CustomError(String),
    MultipleErrors(Vec<ResourceError>),
}

impl ResourceError {
    /// Checks whether resource type error is REST NotFound or not
    pub(crate) fn not_found_rest_json_error(self) -> Result<bool, Self> {
        let err = match self {
            ResourceError::RestJsonError(e) => e,
            _ => {
                return Err(self);
            }
        };
        match err {
            openapi::clients::tower::Error::Response(e1) => {
                if e1.status().as_u16() == 404 {
                    return Ok(true);
                }
                Err(ResourceError::RestJsonError(
                    openapi::clients::tower::Error::Response(e1),
                ))
            }
            _ => Err(ResourceError::RestJsonError(err)),
        }
    }
}

impl From<std::io::Error> for ResourceError {
    fn from(e: std::io::Error) -> ResourceError {
        ResourceError::IOError(e)
    }
}

impl From<String> for ResourceError {
    fn from(e: String) -> ResourceError {
        ResourceError::CustomError(e)
    }
}

impl From<openapi::clients::tower::Error<openapi::models::RestJsonError>> for ResourceError {
    fn from(e: openapi::clients::tower::Error<openapi::models::RestJsonError>) -> ResourceError {
        ResourceError::RestJsonError(e)
    }
}

impl From<serde_json::Error> for ResourceError {
    fn from(e: serde_json::Error) -> ResourceError {
        ResourceError::JSONError(e)
    }
}

impl From<uuid::Error> for ResourceError {
    fn from(e: uuid::Error) -> ResourceError {
        ResourceError::UUIDParseError(e)
    }
}
