use schemars::JsonSchema;
use std::convert::TryFrom;

/// A quantity represented as a humansize with up to 1 floating point.
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq)]
pub struct Quantity(pub u64);
impl Quantity {
    /// Create a [`Self`] from bytes.
    pub fn from_bytes(bytes: u64) -> Self {
        Self(bytes)
    }
    /// Get the bytes.
    pub fn bytes(&self) -> u64 {
        self.0
    }
}
impl TryFrom<&str> for Quantity {
    type Error = parse_size::Error;

    fn try_from(quantity: &str) -> Result<Self, Self::Error> {
        parse_size::parse_size(quantity).map(Self)
    }
}

impl std::fmt::Display for Quantity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", utils::bytes::into_human(self.0))
    }
}

impl<'de> serde::Deserialize<'de> for Quantity {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Quantity::try_from(String::deserialize(deserializer)?.as_str())
            .map_err(serde::de::Error::custom)
    }
}
impl serde::Serialize for Quantity {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl JsonSchema for Quantity {
    fn schema_name() -> String {
        "Quantity".to_owned()
    }

    fn json_schema(__gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        schemars::schema::Schema::Object(schemars::schema::SchemaObject {
            metadata: Some(Box::new(schemars::schema::Metadata {
                description: Some(
                    "Quantity represents a byte quantity with an appropriate SI unit, up to 1 decimal point".to_owned(),
                ),
                examples: vec![
                    "100 GiB".into(),
                    "1.4 MiB".into()
                ],
                ..Default::default()
            })),
            instance_type: Some(schemars::schema::SingleOrVec::Single(Box::new(
                schemars::schema::InstanceType::String,
            ))),
            ..Default::default()
        })
    }
}

#[test]
fn test() {
    #[derive(Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, JsonSchema)]
    struct Struct {
        q: Quantity,
    }
    fn assert_size(bytes: u64, q_str: &str, roundtrip_bytes: u64) {
        let q = Quantity::from_bytes(bytes);
        assert_eq!(q.bytes(), bytes);
        assert_eq!(q.to_string(), q_str);
        assert_eq!(Quantity::try_from(q_str).unwrap().to_string(), q_str);

        let dsp = Struct { q };
        let dsp_val = serde_json::json!({
            "q": q_str
        });
        assert_eq!(serde_json::to_string(&dsp).unwrap(), dsp_val.to_string());
        assert_eq!(
            serde_json::from_value::<Struct>(dsp_val).unwrap().q.bytes(),
            roundtrip_bytes
        );
    }
    let mb = 1024 * 1024;
    let gb = mb * 1024;

    assert_size(100 * mb, "100 MiB", 100 * mb);
    assert_size(gb, "1 GiB", gb);
    assert_size(gb + 20 * mb, "1 GiB", gb);
    assert_size(gb + 25 * mb, "1 GiB", gb);
    assert_size(gb + 200 * mb, "1.2 GiB", 1288490189);
    assert_size(1500 * 1000 * 1000, "1.4 GiB", 1503238554);
}

#[test]
fn quantity_equality_is_exact() {
    let mb = 1024 * 1024;
    let gb = mb * 1024;

    assert_ne!(Quantity::from_bytes(gb), Quantity::from_bytes(gb + 25 * mb));
    assert_eq!(Quantity::from_bytes(gb), Quantity::from_bytes(gb));
}
