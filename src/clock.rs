//! The wall clock blocklyd stamps records, files and answers with.

use time::OffsetDateTime;

pub(crate) fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}
