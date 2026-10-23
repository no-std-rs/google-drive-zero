use alloc::{string::String, vec::Vec};
use serde::{Deserialize, Deserializer};

/// Drive names need not be unique. Persist file IDs, not names, as identity.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub id: String,
    pub name: String,
    pub mime_type: String,
    #[serde(default, deserialize_with = "optional_u64")]
    pub size: Option<u64>,
    #[serde(default, deserialize_with = "optional_u64")]
    pub version: Option<u64>,
    #[serde(default)]
    pub parents: Vec<String>,
    pub modified_time: Option<String>,
    pub md5_checksum: Option<String>,
    pub head_revision_id: Option<String>,
    pub capabilities: Option<Capabilities>,
}
impl File {
    pub fn is_folder(&self) -> bool {
        self.mime_type == "application/vnd.google-apps.folder"
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Capabilities {
    pub can_download: Option<bool>,
}

/// An empty files array can still have a next page. Check next_page_token when
/// iterating, and check incomplete_search before treating a listing as complete.
#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct FilePage {
    #[serde(default)]
    pub files: Vec<File>,
    pub next_page_token: Option<String>,
    #[serde(default)]
    pub incomplete_search: bool,
}

fn optional_u64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    Option::<String>::deserialize(d)?
        .map(|s| s.parse().map_err(serde::de::Error::custom))
        .transpose()
}
