/// Implement Message trait for the type `S` with the message id `I`.
/// # Example
/// ```
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyRequest {}
/// impl_message!(DummyRequest, DummyId);
/// ```
#[macro_export]
macro_rules! impl_message {
    ($S:ident) => {
        impl_message!($S, $S);
    };
    ($S:ident, $I:ident) => {
        #[async_trait::async_trait]
        impl Message for $S {
            impl_channel_id!($I);
        }
    };
}

/// Implement request for all objects of `Type`.
#[macro_export]
macro_rules! impl_vector_request {
    ($Request:ident, $Inner:ident) => {
        /// Request all the `Inner` elements
        #[derive(Serialize, Deserialize, Default, Debug, Clone)]
        pub struct $Request(pub Vec<$Inner>);
        impl $Request {
            /// returns the first element of the tuple and consumes self
            pub fn into_inner(self) -> Vec<$Inner> {
                self.0
            }
        }
    };
}

/// Implement request for all objects of `Type` that require pagination.
#[macro_export]
macro_rules! impl_vector_request_token {
    ($Request:ident, $Inner:ident) => {
        /// Request all the `Inner` elements
        #[derive(Serialize, Deserialize, Default, Debug, Clone)]
        pub struct $Request {
            /// Vector of entries
            pub entries: Vec<$Inner>,
            /// The token to use in subsequent requests.
            pub next_token: Option<u64>,
        }
    };
}
