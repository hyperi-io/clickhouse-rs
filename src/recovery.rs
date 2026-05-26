//! Best-effort row extraction from ClickHouse error messages.
//!
//! Matches two patterns: `is violated at row N` (constraint) and
//! `(at row N)` past row 0 (RowBinary parse). `(at row 1)` returns
//! `None` -- that's the unreliable whole-payload-malformed case.
//! Server wording isn't a stable contract; treat hits as hints,
//! `None` as "fall back to bisection".

use crate::error::Error;

/// Extract row position + category from a server error.
///
/// `None` for transport errors or unrecognised wording. Row is
/// 1-indexed within the batch CH received -- with
/// [`Client::insert_batch_with_isolation`][crate::Client::insert_batch_with_isolation]
/// that's a one-row INSERT, so `row == 1`; identity comes from
/// [`BatchInsertResult::failed`][crate::batch_isolation::BatchInsertResult::failed].
/// `category` stays useful for routing
/// (constraint -> dead-letter, parse -> investigate).
///
/// # Distributed targets -- row position is shard-local
///
/// For `Distributed` engine targets the server fans the INSERT out
/// to shards and the reported row number is the **shard-local** row
/// count, not the coordinator-batch row count. The `row` field will
/// not map back to the client batch position. Treat as an opaque
/// diagnostic when the target table is Distributed. The
/// `category` field is still meaningful (constraint vs parse).
/// Detection of "is this Distributed?" is the caller's job today.
#[must_use]
pub fn failing_row_from_error(err: &Error) -> Option<FailureLocation> {
    let msg = match err {
        Error::BadResponse(s) => s.as_str(),
        _ => return None,
    };
    parse_failure_location(msg)
}

/// Parsed location of a failed row.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FailureLocation {
    /// 1-indexed (CH convention).
    pub row: u64,
    pub category: FailureCategory,
}

/// Failure class for caller routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FailureCategory {
    /// CHECK / NOT NULL / similar server-side validation. Retry
    /// won't help -- quarantine.
    Constraint,
    /// RowBinary parser couldn't read the bytes. May be data,
    /// may be a serialiser bug -- quarantine and investigate.
    Parse,
}

fn parse_failure_location(msg: &str) -> Option<FailureLocation> {
    // Constraint first: a CHECK message can also include "(at row N)"
    // in stack frames, and the parse matcher would mis-claim it.
    if let Some(after) = find_after(msg, "is violated at row ")
        && let Some(row) = parse_uint(after)
    {
        return Some(FailureLocation {
            row,
            category: FailureCategory::Constraint,
        });
    }

    if let Some(after) = find_after(msg, "(at row ")
        && let Some(row) = parse_uint(after)
    {
        // (at row 1) means "whole payload malformed from byte 0".
        // Unreliable -- caller falls back to bisection.
        if row == 1 {
            return None;
        }
        return Some(FailureLocation {
            row,
            category: FailureCategory::Parse,
        });
    }

    None
}

fn find_after<'a>(haystack: &'a str, needle: &str) -> Option<&'a str> {
    let idx = haystack.find(needle)?;
    Some(&haystack[idx + needle.len()..])
}

fn parse_uint(s: &str) -> Option<u64> {
    let end = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    s.get(..end)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CH 26.2.4.23 (captured 2026-05-07).
    const CHECK_VIOLATION_ROW_5000: &str =
        "Code: 469. DB::Exception: Constraint `x_lt_10` for table \
         benchmark.__test_atomic_batch_failure (684d126a-...) is \
         violated at row 5000. Expression: (x < 10). Column values: \
         x = 100: While executing WaitForAsyncInsert. \
         (VIOLATED_CONSTRAINT) (version 26.2.4.23 (official build))";

    /// CHECK at row 1.
    const CHECK_VIOLATION_ROW_1: &str =
        "Code: 469. DB::Exception: Constraint `x_lt_10` is violated \
         at row 1. Expression: (x < 10). Column values: x = 200";

    /// RowBinary parse error.
    const PARSE_ERROR_ROW_5000: &str =
        "Code: 33. DB::Exception: Cannot read all data. Bytes read: \
         3. Bytes expected: 4: (at row 5000)\n: While executing \
         BinaryRowInputFormat. (CANNOT_READ_ALL_DATA)";

    /// Unreliable (at row 1): whole payload malformed from byte 0.
    const UNRELIABLE_ROW_1_PARSE: &str =
        "Code: 33. DB::Exception: Cannot read all data. Bytes read: \
         5. Bytes expected: 8.: (at row 1) : While executing \
         BinaryRowInputFormat. (CANNOT_READ_ALL_DATA)";

    /// Error with no row marker.
    const NO_ROW_INFO: &str =
        "Code: 60. DB::Exception: Table benchmark.t does not exist.";

    #[test]
    fn extracts_constraint_row_5000() {
        let err = Error::BadResponse(CHECK_VIOLATION_ROW_5000.into());
        assert_eq!(
            failing_row_from_error(&err),
            Some(FailureLocation {
                row: 5000,
                category: FailureCategory::Constraint,
            })
        );
    }

    #[test]
    fn extracts_constraint_row_1() {
        let err = Error::BadResponse(CHECK_VIOLATION_ROW_1.into());
        assert_eq!(
            failing_row_from_error(&err),
            Some(FailureLocation {
                row: 1,
                category: FailureCategory::Constraint,
            })
        );
    }

    #[test]
    fn extracts_parse_row_5000() {
        let err = Error::BadResponse(PARSE_ERROR_ROW_5000.into());
        assert_eq!(
            failing_row_from_error(&err),
            Some(FailureLocation {
                row: 5000,
                category: FailureCategory::Parse,
            })
        );
    }

    #[test]
    fn parse_at_row_1_returns_none_unreliable_signal() {
        let err = Error::BadResponse(UNRELIABLE_ROW_1_PARSE.into());
        assert_eq!(failing_row_from_error(&err), None);
    }

    #[test]
    fn no_row_info_returns_none() {
        let err = Error::BadResponse(NO_ROW_INFO.into());
        assert_eq!(failing_row_from_error(&err), None);
    }

    #[test]
    fn non_bad_response_returns_none() {
        let err = Error::Custom("custom".into());
        assert_eq!(failing_row_from_error(&err), None);
    }

    #[test]
    fn empty_bad_response_returns_none() {
        let err = Error::BadResponse(String::new());
        assert_eq!(failing_row_from_error(&err), None);
    }

    /// row 0 is not coerced -- pins that we don't filter
    /// constraint row=0 the way we filter parse row=1.
    #[test]
    fn constraint_row_0_is_returned_as_is() {
        let msg = "Code: 469. DB::Exception: Constraint `x_lt_10` is \
                   violated at row 0. Expression: (x < 10). \
                   Column values: x = 100";
        let err = Error::BadResponse(msg.into());
        assert_eq!(
            failing_row_from_error(&err),
            Some(FailureLocation {
                row: 0,
                category: FailureCategory::Constraint,
            })
        );
    }

    /// Both patterns present -- constraint wins (tried first).
    #[test]
    fn constraint_match_wins_when_both_patterns_present() {
        let msg = "Code: 469. DB::Exception: Constraint `x_lt_10` is \
                   violated at row 7. Column values: x = 100. \
                   Stack trace: ... (at row 99) ... (CANNOT_READ_ALL_DATA)";
        let err = Error::BadResponse(msg.into());
        assert_eq!(
            failing_row_from_error(&err),
            Some(FailureLocation {
                row: 7,
                category: FailureCategory::Constraint,
            })
        );
    }

    #[test]
    fn extracts_constraint_row_very_large() {
        let big = u64::MAX - 1;
        let msg = format!(
            "Code: 469. DB::Exception: Constraint `x_lt_10` is \
             violated at row {big}. Expression: (x < 10)."
        );
        let err = Error::BadResponse(msg);
        assert_eq!(
            failing_row_from_error(&err),
            Some(FailureLocation {
                row: big,
                category: FailureCategory::Constraint,
            })
        );
    }

    /// 21-digit row number (> u64::MAX) -> None.
    #[test]
    fn out_of_range_row_returns_none() {
        let msg = "Code: 469. DB::Exception: Constraint `x_lt_10` is \
                   violated at row 99999999999999999999. Expression: \
                   (x < 10).";
        let err = Error::BadResponse(msg.into());
        assert_eq!(failing_row_from_error(&err), None);
    }
}
