use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::Arc,
};

use chrono::DateTime;

use crate::ReadError;

pub type MsgKeyMap<T> = HashMap<MsgKey, T, HashBuilder>;
pub type MsgKeySet = HashSet<MsgKey, HashBuilder>;
pub type HashBuilder = xxhash_rust::xxh3::Xxh3Builder;

struct MsgKeyRaw<'a> {
    info_meta_service: &'a str,
    info_meta_env: &'a str,
    /// This is the original timestamp field, which will be formatted into a date upon
    info_timestamp: &'a str,
}

#[derive(Debug, Clone, Eq)]
pub struct MsgKey {
    /// Cached hash value. Must be the same for any two equal strings
    hash: u64,
    name: Arc<str>,
}

impl PartialEq for MsgKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Hash for MsgKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.hash.hash(state)
    }
}

impl MsgKey {
    fn from_raw(r: &MsgKeyRaw) -> Self {
        // Get the YYYY-MM-DD
        let datetime = DateTime::parse_from_rfc3339(r.info_timestamp).unwrap();
        let date = datetime.date_naive().format("%Y-%m-%d");

        let name = format!("{}_{}_{}", r.info_meta_service, r.info_meta_env, date);
        let mut hasher = HashBuilder::default().build();
        name.hash(&mut hasher);
        Self {
            name: Arc::from(name.as_str()),
            hash: hasher.finish(),
        }
    }
    pub fn path_to(&self, root: &Path) -> PathBuf {
        let mut p = root.join(&*self.name);
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

#[cfg(test)]
mod tests {
    use rand::{distributions::Standard, thread_rng, Rng};

    use crate::data::{HashBuilder, MsgKey, MsgKeyRaw};
    use std::hash::{BuildHasher, Hash, Hasher};

    #[test]
    fn test_msg_key_hash_equivalence() {
        #[track_caller]
        fn check(raw: &MsgKeyRaw) {
            let k = MsgKey::from_raw(raw);

            let b = HashBuilder::new().with_seed(rand::random());
            let mut state0 = b.build_hasher();
            let mut state1 = b.build_hasher();

            k.hash(&mut state0);
            k.hash(&mut state1);

            assert_eq!(state0.finish(), state1.finish());
        }

        check(&MsgKeyRaw {
            info_meta_service: "foo",
            info_meta_env: "thsugfsdfgsdfg",
            info_timestamp: "2022-12-17T17:57:08.129711647+00:00",
        });

        for _ in 0..100 {
            check(&MsgKeyRaw {
                info_meta_service: thread_rng()
                    .sample_iter::<u8, _>(Standard)
                    .map(char::from)
                    .take(10)
                    .collect::<String>()
                    .as_str(),
                info_meta_env: thread_rng()
                    .sample_iter::<u8, _>(Standard)
                    .map(char::from)
                    .take(10)
                    .collect::<String>()
                    .as_str(),
                info_timestamp: "2022-12-17T17:57:08.129711647+00:00",
            });
        }
    }
}
