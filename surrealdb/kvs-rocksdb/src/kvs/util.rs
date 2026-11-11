use std::ops::Range;

use super::direction::Direction;
use crate::kvs::Key;

/// Advances a key to the next value,
/// can be used to skip over a certain key.
pub fn advance_key(key: &mut [u8]) {
	for b in key.iter_mut().rev() {
		*b = b.wrapping_add(1);
		if *b != 0 {
			break;
		}
	}
}

/// Advance a resume-by-bound cursor's range so the next chunk resumes strictly
/// after `last` (the last key visited this chunk). Mirrors the successor logic
/// in `DefaultValsCursor`/`DefaultKeysCursor`: forward appends `0x00` to get the
/// minimal key greater than `last` (the half-open `start` already excludes it
/// otherwise); backward clips `end` to `last` (the half-open `end` already
/// excludes it). A `None` `last` means nothing was visited — leave the range.
pub(crate) fn update_range(rng: &mut Range<Key>, dir: Direction, last: Option<&[u8]>) {
	let Some(last) = last else {
		return;
	};
	match dir {
		Direction::Forward => {
			rng.start.clear();
			rng.start.extend_from_slice(last);
			rng.start.push(0x00);
		}
		Direction::Backward => {
			rng.end.clear();
			rng.end.extend_from_slice(last);
		}
	}
}

/// Return the byte-prefix range used by the legacy owned-key API.
pub fn to_prefix_range(key: &[u8]) -> super::err::Result<Range<Key>> {
	let start = key.to_vec();
	let mut end = start.clone();
	end.push(0xff);
	Ok(start..end)
}
