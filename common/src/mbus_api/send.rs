/// Main Message trait, which should typically be used to send
/// MessageBus messages.
/// Implements Message trait for the type `S` with the reply type
/// `R`, the message id `I`, the default channel `C`.
/// If specified it makes use of the Request/Publish traits exported
/// by type `T`, otherwise it defaults to using `S`.
/// Also implements the said Request/Publish traits for type `T`, if
/// specified, otherwise it implements them for type `S`.
///
/// # Example
/// ```
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyRequest {}
/// bus_impl_message_all!(DummyRequest, DummyId, (), DummyChan);
///
/// let reply = DummyRequest { }.request().await.unwrap();
/// ```
#[macro_export]
macro_rules! bus_impl_message_all {
    ($S:ident, $I:ident, $R:tt, $C:ident) => {
        bus_impl_message!($S, $I, $R, $C);
    };
    ($S:ident, $I:ident, $R:tt, $C:ident, $T:ident) => {
        bus_impl_message!($S, $I, $R, $C, $T);
    };
}

/// Implement Message trait for the type `S` with the reply type
/// `R`, the message id `I`, the default channel `C`.
/// If specified it makes use of the Request/Publish traits exported
/// by type `T`, otherwise it defaults to using `S`.
/// # Example
/// ```
/// #[derive(Serialize, Deserialize, Debug, Default, Clone)]
/// struct DummyRequest {}
/// bus_impl_message!(DummyRequest, DummyId, (), DummyChan);
/// ```
#[macro_export]
macro_rules! bus_impl_message {
    ($S:ident, $I:ident, $R:tt, $C:ident) => {
        bus_impl_message!($S, $I, $R, $C, $S);
    };
    ($S:ident, $I:ident, $R:tt, $C:ident, $T:ident) => {
        #[async_trait::async_trait]
        impl Message for $S {
            impl_channel_id!($I, $C);
        }
    };
}

/// Implement request for all objects of `Type`
#[macro_export]
macro_rules! bus_impl_vector_request {
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
macro_rules! bus_impl_vector_request_token {
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
