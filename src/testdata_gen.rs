use std::io::Write;

use chrono::{NaiveDate, NaiveDateTime};
use flate2::{write::GzEncoder, Compression};

use self::gen_format::FullLine;

#[derive(Debug, Clone)]
pub struct TestdataCfg {
    pub lines: usize,
    pub compression: Compression,
    /// All dates generated will fall within this range, with no guarantee
    pub date_range: std::ops::Range<NaiveDate>,
}

impl TestdataCfg {
    #[track_caller]
    pub fn set_start_date(&mut self, y: i32, m: u32, d: u32) -> &mut Self {
        self.date_range.start = NaiveDate::from_ymd_opt(y, m, d).unwrap();
        self
    }
    #[track_caller]
    pub fn set_end_date(&mut self, y: i32, m: u32, d: u32) -> &mut Self {
        self.date_range.end = NaiveDate::from_ymd_opt(y, m, d).unwrap();
        self
    }
}

impl Default for TestdataCfg {
    fn default() -> Self {
        let mut s = Self {
            lines: 0,
            compression: Default::default(),
            date_range: Default::default(),
        };
        s.set_start_date(2024, 10, 20).set_end_date(2026, 3, 3);
        s
    }
}

/// Generates testdata and writes it to two streams:
///
/// * `w_enc` - The gzipped json data, which would be normally written to a `.json.gz` file
/// * `w_dbg` - The generated json data, human readable
///
/// Printing the output to stdout and ignoring the encoded output:
/// ```
/// generate_testdata(
///     TestdataCfg {
///         lines: 100,
///         ..Default::default()
///     },
///     &mut std::io::sink(),
///     &mut std::io::stdout(),
/// );
///
/// ```
pub fn generate_testdata(
    cfg: TestdataCfg,
    w_enc: &mut impl std::io::Write,
    w_dbg: &mut impl std::io::Write,
) -> Result<(), std::io::Error> {
    let mut enc = GzEncoder::new(w_enc, Compression::default());

    for _l in 0..cfg.lines {
        let ln = format!("{}\n", FullLine::generate(&cfg).to_json());
        enc.write_all(ln.as_bytes())?;
        w_dbg.write_all(ln.as_bytes())?;
    }

    enc.finish()?;
    w_dbg.flush()?;

    Ok(())
}

mod gen_format {
    use rand::prelude::SliceRandom;
    use std::{cell::OnceCell, fmt::Display, sync::OnceLock};

    use chrono::{
        DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime,
        SecondsFormat, TimeDelta,
    };
    use rand::{
        distributions::{Alphanumeric, Distribution, Standard},
        thread_rng, Rng,
    };

    use super::TestdataCfg;

    #[derive(Debug, Clone, Copy)]
    enum Level {
        Debug,
        Info,
        Build,
    }

    impl Distribution<Level> for Standard {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Level {
            [Level::Debug, Level::Info, Level::Build]
                .choose(rng)
                .unwrap()
                .clone()
        }
    }

    impl Display for Level {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(
                f,
                "{}",
                match self {
                    Level::Debug => "debug",
                    Level::Info => "info",
                    Level::Build => "build",
                }
            )
        }
    }

    struct Timestamp {
        t: String,
    }

    impl Timestamp {
        pub fn gen(cfg: &TestdataCfg) -> Self {
            let num_days = (cfg.date_range.end - cfg.date_range.start).num_days();
            let day = thread_rng().gen_range(0..num_days);
            let day = cfg.date_range.start + Duration::days(day);

            let time_secs = thread_rng().gen_range(-86_399..=86_399);
            let time = FixedOffset::west_opt(time_secs).unwrap();
            let time = NaiveTime::from_hms_opt(0, 0, 0).unwrap() + time;

            let tz_hrs = thread_rng().gen_range(-23..=23);
            let tz = FixedOffset::west_opt((tz_hrs) * 3600).unwrap();

            let datetime = NaiveDateTime::new(day, time);
            let datetime = datetime.and_utc().with_timezone(&tz);

            Self {
                t: datetime.to_rfc3339_opts(SecondsFormat::AutoSi, true),
            }
        }
    }

    impl Display for Timestamp {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.t)
        }
    }

    struct Meta {
        service: String,
        env: String,
        user: String,
    }

    impl Meta {
        pub fn gen(_cfg: &TestdataCfg) -> Self {
            static SERVICES: OnceLock<Vec<String>> = OnceLock::new();
            static ENVS: OnceLock<Vec<String>> = OnceLock::new();
            static USERS: OnceLock<Vec<String>> = OnceLock::new();

            /// Gets a random element from a oncelock list
            fn get_once_list_elem(
                s: &OnceLock<Vec<String>>,
                count: usize,
                elem_len: std::ops::Range<usize>,
            ) -> String {
                let idx = thread_rng().gen_range(0..count);
                s.get_or_init(move || {
                    let mut v = vec![];
                    for _ in 0..count {
                        let s_len = thread_rng().gen_range(elem_len.clone());
                        let s = thread_rng()
                            .sample_iter(&Alphanumeric)
                            .take(s_len)
                            .map(char::from)
                            .collect();
                        v.push(s);
                    }
                    v
                })[idx]
                    .clone()
            }

            Self {
                service: get_once_list_elem(&SERVICES, 900, 3..6),
                env: get_once_list_elem(&ENVS, 3, 3..6),
                user: get_once_list_elem(&USERS, 1000, 5..15),
            }
        }
    }

    /// A complete json line
    pub(super) struct FullLine {
        message: String,
        timestamp: Timestamp,
        level: Level,
        meta: Meta,
    }

    impl FullLine {
        /// Generates a random line given the context
        pub fn generate(cfg: &TestdataCfg) -> Self {
            let msg_len = thread_rng().gen_range(10..100);
            Self {
                message: thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(msg_len)
                    .map(char::from)
                    .collect(),
                timestamp: Timestamp::gen(cfg),
                level: thread_rng().gen(),
                meta: Meta::gen(cfg),
            }
        }
        pub fn to_json(&self) -> String {
            let j = json::object! {
                message: self.message.clone(),
                "@timestamp": self.timestamp.to_string(),
                level: self.level.to_string(),
                "@meta": {
                    service: self.meta.service.clone(),
                    env: self.meta.env.clone(),
                    user: self.meta.user.clone(),
                }
            };

            j.dump()
        }
    }
}
