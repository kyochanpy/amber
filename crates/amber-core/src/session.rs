use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::{Uuid, Version};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    pub fn new() -> Self {
        Self(Uuid::now_v7().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn parse(value: impl AsRef<str>) -> Result<Self, SessionIdError> {
        value.as_ref().parse()
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl AsRef<str> for SessionId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SessionId {
    type Err = SessionIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid = Uuid::parse_str(value).map_err(|source| SessionIdError::InvalidUuid {
            value: value.to_owned(),
            source,
        })?;

        if uuid.get_version() != Some(Version::SortRand) {
            return Err(SessionIdError::WrongVersion {
                value: value.to_owned(),
            });
        }

        Ok(Self(uuid.to_string()))
    }
}

#[derive(Debug, Error)]
pub enum SessionIdError {
    #[error("invalid session ID '{value}': {source}")]
    InvalidUuid {
        value: String,
        #[source]
        source: uuid::Error,
    },
    #[error("session ID '{value}' is not a UUIDv7")]
    WrongVersion { value: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_session_ids_use_uuid_v7_and_sort_by_time() {
        let first = SessionId::new();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let second = SessionId::new();

        let first_uuid = Uuid::parse_str(first.as_str()).expect("first session ID should parse");
        let second_uuid = Uuid::parse_str(second.as_str()).expect("second session ID should parse");

        assert_eq!(first_uuid.get_version(), Some(Version::SortRand));
        assert_eq!(second_uuid.get_version(), Some(Version::SortRand));
        assert!(
            first < second,
            "UUIDv7 strings should sort by creation time"
        );
    }

    #[test]
    fn parse_rejects_non_v7_uuids() {
        let error = SessionId::parse("550e8400-e29b-41d4-a716-446655440000")
            .expect_err("v4 UUID should be rejected");

        assert!(matches!(error, SessionIdError::WrongVersion { .. }));
    }
}
