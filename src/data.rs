use std::{
    fmt::Display,
    path::{Path, PathBuf},
};

use chrono::DateTime;

use crate::ReadError;

struct MsgKeyRaw<'a> {
    info_meta_service: &'a str,
    info_meta_env: &'a str,
    /// This is the original timestamp field, which will be formatted into a date upon
    info_timestamp: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MsgKey {
    name: String,
}

impl MsgKey {
    fn from_raw(r: &MsgKeyRaw) -> Self {
        // Get the YYYY-MM-DD
        let datetime = DateTime::parse_from_rfc3339(r.info_timestamp).unwrap();
        let date = datetime.date_naive().format("%Y-%m-%d");

        let name = format!(
            "{}_{}_{}",
            r.info_meta_service,
            r.info_meta_env,
            // In the original, the timestamp had additional formatting...
            date
        );
        Self { name }
    }
    pub fn path_to(&self, root: &Path) -> PathBuf {
        let mut p = root.join(&self.name);
        p.set_extension("json.gz");
        p
    }
}

/// Stores the relevant data of a given line, along with the original string.
/// The original string mut contain a newline at the end
///
/// Data extracted:
/// * The key which fully determines the output file this line goes to
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineData {
    orig: String,
    key: MsgKey,
}

impl Display for LineData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.orig)
    }
}

impl LineData {
    #[inline(always)]
    pub fn end_of_input() -> Self {
        Self {
            orig: "EOI".into(),
            key: MsgKey { name: "EOI".into() },
        }
    }
    pub fn is_eoi(&self) -> bool {
        &Self::end_of_input() == self
    }
    pub fn key(&self) -> &MsgKey {
        &self.key
    }
    pub fn original_line_text(&self) -> &str {
        &self.orig
    }
    /// Creates a new `LineData` from the given `line`, which does *not* contain a newline.
    /// A newline will be added to end end of this `LineData`
    pub fn parse(line: &str) -> Result<Self, ReadError> {
        let info = match json::parse(line) {
            Ok(val) => val,
            Err(_) => {
                return Err(ReadError::InvalidLine(line.to_string()));
            }
        };

        let meta = &info["@meta"];

        Ok(LineData {
            orig: format!("{}\n", line),
            key: MsgKey::from_raw(&MsgKeyRaw {
                info_meta_service: meta["service"].as_str().expect(&format!(
                    "Expected `info.@meta.service` as a string: {line}"
                )),
                info_meta_env: meta["env"]
                    .as_str()
                    .expect(&format!("Expected `info.@meta.env` as a string: {line}")),
                info_timestamp: info["@timestamp"]
                    .as_str()
                    .expect(&format!("Expected `info.@timestamp` as a string: {line}")),
            }),
        })
    }
}
