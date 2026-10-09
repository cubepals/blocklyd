//! The wall clock blocklyd stamps records, files and answers with.

use time::OffsetDateTime;

pub fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}
