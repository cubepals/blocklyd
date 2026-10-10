// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! The wall clock blocklyd stamps records, files and answers with.

use time::OffsetDateTime;

pub(crate) fn now() -> OffsetDateTime {
    OffsetDateTime::now_utc()
}
