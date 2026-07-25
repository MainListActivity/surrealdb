//! Conversion from core [`Error`] to wire-friendly [`surrealdb_types::Error`].
//!
//! This is the single place that defines how embedded database errors are mapped to the
//! public types-layer error used over RPC and in the SDK.

use std::error::Error as StdError;

use surrealdb_types::{
	AlreadyExistsError, AuthError, ConfigurationError, ConnectionError, Error as TypesError,
	NotAllowedError, NotFoundError, QueryError, QuotaError, SerdeWrapper, SerializationError,
	SurrealValue, ToSql, ValidationError, Value,
};

use crate::err::Error;
use crate::iam::Error as IamErrorKind;
use crate::kvs::Error as KvsError;

/// Converts a core database error into the public wire-friendly error type.
///
/// Takes ownership so owned data (e.g. message strings, IAM details) can be moved instead of
/// cloned. For `anyhow::Error`, use `downcast` to consume and recover the core `Error`:
/// `e.downcast::<Error>().map(into_types_error).unwrap_or_else(|e|
/// TypesError::internal(e.to_string()))`.
pub fn into_types_error(error: Error) -> TypesError {
	use Error::*;
	let message = error.to_string();
	let source = error.source().map(|s| TypesError::internal(s.to_string()));
	let mapped = match error {
		// Auth
		ExpiredSession => TypesError::not_allowed(message, AuthError::SessionExpired),
		ExpiredToken => TypesError::not_allowed(message, AuthError::TokenExpired),
		InvalidAuth => TypesError::not_allowed(message, AuthError::InvalidAuth),
		UnexpectedAuth => TypesError::not_allowed(message, AuthError::UnexpectedAuth),
		MissingUserOrPass => TypesError::not_allowed(message, AuthError::MissingUserOrPass),
		NoSigninTarget => TypesError::not_allowed(message, AuthError::NoSigninTarget),
		InvalidPass => TypesError::not_allowed(message, AuthError::InvalidPass),
		TokenMakingFailed => TypesError::not_allowed(message, AuthError::TokenMakingFailed),
		IamError(iam_err) => match iam_err {
			IamErrorKind::InvalidRole(name) => TypesError::not_allowed(
				message,
				AuthError::InvalidRole {
					name,
				},
			),
			IamErrorKind::NotAllowed {
				actor,
				action,
				resource,
			} => TypesError::not_allowed(
				message,
				AuthError::NotAllowed {
					actor,
					action,
					resource,
				},
			),
		},
		InvalidSignup => TypesError::not_allowed(message, AuthError::InvalidSignup),

		// Validation
		NsEmpty => TypesError::validation(message, ValidationError::NamespaceEmpty),
		DbEmpty => TypesError::validation(message, ValidationError::DatabaseEmpty),
		InvalidQuery(_) => TypesError::validation(message, None),
		InvalidParam {
			name,
		} => TypesError::validation(
			message,
			ValidationError::InvalidParameter {
				name,
			},
		),
		InvalidContent {
			value,
		} => TypesError::validation(
			message,
			ValidationError::InvalidContent {
				value: value.to_sql(),
			},
		),
		InvalidMerge {
			value,
		} => TypesError::validation(
			message,
			ValidationError::InvalidMerge {
				value: value.to_sql(),
			},
		),
		InvalidPatch(_) => TypesError::validation(message, None),
		Coerce(_) => TypesError::validation(message, None),
		Cast(_) => TypesError::validation(message, None),
		TryAdd(..) | TrySub(..) | TryMul(..) | TryDiv(..) | TryRem(..) | TryPow(..) | TryNeg(_)
		| TryExtend(_) => TypesError::validation(message, None),
		TryFrom(..) => TypesError::validation(message, None),
		DuplicatedMatchRef {
			..
		} => TypesError::validation(message, None),
		AccessUnsupportedAlgorithm => TypesError::validation(message, None),

		// Not allowed (method, scripting, function, net target)
		ScriptingNotAllowed => TypesError::not_allowed(message, NotAllowedError::Scripting),
		FunctionNotAllowed(name) => TypesError::not_allowed(
			message,
			NotAllowedError::Function {
				name,
			},
		),
		NetTargetNotAllowed(name) => TypesError::not_allowed(
			message,
			NotAllowedError::Target {
				name,
			},
		),

		// Configuration
		RealtimeDisabled => {
			TypesError::configuration(message, ConfigurationError::LiveQueryNotSupported)
		}

		// Query
		QueryTimedout(duration) => TypesError::query(
			message,
			QueryError::TimedOut {
				duration: duration.0,
			},
		),
		TransactionTimedout(duration) => TypesError::query(
			message,
			QueryError::TimedOut {
				duration: duration.0,
			},
		),
		QueryCancelled => TypesError::query(message, QueryError::Cancelled),
		QueryNotExecuted {
			message,
		} => TypesError::query(message, QueryError::NotExecuted),
		AccessRecordSignupQueryFailed | AccessRecordSigninQueryFailed => {
			TypesError::query(message, None)
		}
		AccessRecordNoSignup | AccessRecordNoSignin => TypesError::not_allowed(message, None),

		// Serialization
		Unencodable => TypesError::serialization(message, None),
		Storekey(_) => TypesError::serialization(message, None),
		Revision(_) => TypesError::serialization(message, None),
		Utf8Error(_) => TypesError::serialization(message, None),
		Serialization(..) => TypesError::serialization(message, SerializationError::Serialization),

		// Not found
		NsNotFound {
			name,
		} => TypesError::not_found(
			message,
			NotFoundError::Namespace {
				name,
			},
		),
		DbNotFound {
			name,
		} => TypesError::not_found(
			message,
			NotFoundError::Database {
				name,
			},
		),
		TbNotFound {
			name,
		} => TypesError::not_found(
			message,
			NotFoundError::Table {
				name: name.into_string(),
			},
		),
		IdNotFound {
			rid,
		} => TypesError::not_found(
			message,
			NotFoundError::Record {
				id: rid,
			},
		),

		// Already exists
		DbAlreadyExists {
			name,
		} => TypesError::already_exists(
			message,
			AlreadyExistsError::Database {
				name,
			},
		),
		NsAlreadyExists {
			name,
		} => TypesError::already_exists(
			message,
			AlreadyExistsError::Namespace {
				name,
			},
		),
		TbAlreadyExists {
			name,
		} => TypesError::already_exists(
			message,
			AlreadyExistsError::Table {
				name,
			},
		),
		RecordExists {
			record,
		} => TypesError::already_exists(
			message,
			AlreadyExistsError::Record {
				id: record.to_sql(),
			},
		),
		ClAlreadyExists {
			..
		} => TypesError::internal(message),
		ApAlreadyExists {
			..
		} => TypesError::internal(message),
		AzAlreadyExists {
			..
		} => TypesError::internal(message),
		BuAlreadyExists {
			..
		} => TypesError::internal(message),
		EvAlreadyExists {
			..
		}
		| FdAlreadyExists {
			..
		}
		| FcAlreadyExists {
			..
		}
		| MdAlreadyExists {
			..
		}
		| IxAlreadyExists {
			..
		}
		| MlAlreadyExists {
			..
		}
		| PaAlreadyExists {
			..
		}
		| CgAlreadyExists {
			..
		}
		| SeqAlreadyExists {
			..
		}
		| NtAlreadyExists {
			..
		}
		| DtAlreadyExists {
			..
		}
		| UserRootAlreadyExists {
			..
		}
		| UserNsAlreadyExists {
			..
		}
		| UserDbAlreadyExists {
			..
		}
		| AccessRootAlreadyExists {
			..
		}
		| AccessNsAlreadyExists {
			..
		}
		| AccessDbAlreadyExists {
			..
		}
		| IndexAlreadyBuilding {
			..
		}
		| IndexingBuildingCancelled {
			..
		} => TypesError::internal(message),

		// Thrown
		Thrown(..) => TypesError::thrown(message),

		// Connection/transport (remote request failure)
		Http(..) => TypesError::connection(message, ConnectionError::ConnectionFailed),

		// Not found (no record returned)
		NoRecordFound => TypesError::not_found(message, None),

		// KVS: preserve type information for wire and client retry/UX
		Kvs(kvs_err) => match kvs_err {
			KvsError::TransactionConflict(_) => {
				TypesError::query(message, QueryError::TransactionConflict)
			}
			KvsError::ConnectionFailed(_) => {
				TypesError::connection(message, ConnectionError::ConnectionFailed)
			}
			KvsError::TransactionKeyAlreadyExists => TypesError::already_exists(message, None),
			KvsError::ReadAndDeleteOnly => TypesError::not_allowed(message, None),
			KvsError::TransactionTooLarge
			| KvsError::TransactionKeyTooLarge
			| KvsError::TransactionRangeTooLarge(_) => {
				TypesError::validation(message, ValidationError::InvalidParams)
			}
			KvsError::TransactionFinished
			| KvsError::TransactionReadonly
			| KvsError::TransactionConditionNotMet => TypesError::query(message, None),
			KvsError::UnsupportedVersionedQueries => TypesError::configuration(message, None),
			KvsError::Datastore(_)
			| KvsError::Transaction(_)
			| KvsError::TimestampInvalid(_)
			| KvsError::Internal(_)
			| KvsError::CompactionNotSupported => TypesError::internal(message),
		},

		QuotaAlreadyExists {
			database,
		} => TypesError::quota(
			message,
			QuotaError::new(
				"quota_policy_exists",
				false,
				Value::Object(surrealdb_types::object! {
					database: database,
				}),
			),
		),
		QuotaNotFound {
			database,
		} => TypesError::quota(
			message,
			QuotaError::new(
				"quota_policy_not_found",
				false,
				Value::Object(surrealdb_types::object! {
					database: database,
				}),
			),
		),
		QuotaGenerationMismatch {
			database,
			expected,
			actual,
		} => TypesError::quota(
			message,
			QuotaError::new(
				"quota_generation_mismatch",
				false,
				Value::Object(surrealdb_types::object! {
					database: database,
					expected: expected,
					actual: actual,
				}),
			),
		),
		QuotaGenerationRequired {
			database,
		} => TypesError::quota(
			message,
			QuotaError::new(
				"quota_generation_required",
				false,
				Value::Object(surrealdb_types::object! {
					database: database,
				}),
			),
		),
		QuotaRuleNotFound {
			id,
		} => TypesError::quota(
			message,
			QuotaError::new(
				"quota_rule_not_found",
				false,
				Value::Object(surrealdb_types::object! {
					rule_id: id,
				}),
			),
		),
		QuotaPolicyInvalid {
			reason,
		} => TypesError::quota(
			message,
			QuotaError::new(
				"quota_policy_invalid",
				false,
				Value::Object(surrealdb_types::object! {
					reason: reason,
				}),
			),
		),
		QuotaImportNotAllowed => TypesError::quota(
			message,
			QuotaError::new("quota_import_not_allowed", false, Value::Object(Default::default())),
		),
		QuotaUsageInvalid {
			..
		} => TypesError::quota(
			message,
			QuotaError::new(
				"quota_ledger_unavailable",
				false,
				Value::Object(surrealdb_types::object! {
					state: "corrupt",
				}),
			),
		),
		QuotaUsageNotReady {
			state,
		} => {
			let retryable = state == "rebuilding";
			TypesError::quota(
				message,
				QuotaError::new(
					"quota_ledger_unavailable",
					retryable,
					Value::Object(surrealdb_types::object! {
						state: state,
					}),
				),
			)
		}
		QuotaConflict => TypesError::quota(
			message,
			QuotaError::new("quota_conflict", true, Value::Object(Default::default())),
		),
		QuotaPolicyChanged {
			database,
			expected,
			actual,
		} => TypesError::quota(
			message,
			QuotaError::new(
				"quota_policy_changed",
				true,
				Value::Object(surrealdb_types::object! {
					database: database,
					expected: expected,
					actual: actual,
				}),
			),
		),
		QuotaExceeded(details) => {
			let violations = details
				.violations
				.into_iter()
				.map(|violation| {
					let delta = SerdeWrapper(violation.delta).into_value();
					Value::Object(surrealdb_types::object! {
						resource: violation.resource,
						table: violation.table,
						rule_ids: vec![violation.rule],
						limit: violation.limit,
						current: violation.current,
						delta: delta,
						projected: violation.projected,
						over_by: violation.over_by,
					})
				})
				.collect::<Vec<_>>();
			let safe_details = Value::Object(surrealdb_types::object! {
				database: details.database,
				generation: details.generation,
				violations: Value::Array(violations.into()),
				truncated: details.truncated,
			});
			TypesError::quota(message, QuotaError::new("quota_exceeded", false, safe_details))
		}

		// Internal and everything else
		Internal(..) => TypesError::internal(message),
		Unimplemented(..) => TypesError::internal(message),
		Io(..) => TypesError::internal(message),
		Channel(..) => TypesError::internal(message),
		CorruptedIndex(_) => TypesError::internal(message),
		NoIndexFoundForMatch {
			..
		} => TypesError::internal(message),
		AnalyzerError(..) => TypesError::internal(message),
		HighlightError(..) => TypesError::internal(message),
		FstError(_) => TypesError::internal(message),
		ObsError(_) => TypesError::internal(message),
		TimestampOverflow(..) => TypesError::internal(message),
		ApiError(error) => error.into_types_error(),

		_ => TypesError::internal(message),
	};

	if let Some(quota) = mapped.quota_details() {
		tracing::warn!(
			target: "surrealdb::core::quota",
			error_code = quota.code(),
			retryable = quota.retryable(),
			"native quota operation failed"
		);
	}

	if let Some(cause) = source {
		mapped.with_cause(cause)
	} else {
		mapped
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{ErrorDetails, SurrealValue, Value};

	use super::*;
	use crate::err::{QuotaExceededError, QuotaViolation};

	fn assert_quota_error(error: Error, code: &str, retryable: bool) -> TypesError {
		let mapped = into_types_error(error);
		let quota = mapped.quota_details().expect("expected quota error details");
		assert_eq!(quota.code(), code);
		assert_eq!(quota.retryable(), retryable);
		mapped
	}

	#[test]
	fn quota_exceeded_is_a_stable_round_trip_wire_error() {
		let mapped = into_types_error(Error::QuotaExceeded(Box::new(QuotaExceededError {
			database: "app".to_owned(),
			generation: 1,
			violations: vec![QuotaViolation {
				rule: "records".to_owned(),
				resource: "record".to_owned(),
				table: "ent_user".to_owned(),
				current: 2,
				delta: 1,
				projected: 3,
				limit: 2,
				over_by: 1,
			}],
			truncated: false,
		})));
		let ErrorDetails::Quota(quota) = mapped.details() else {
			panic!("expected quota error details, got {:?}", mapped.details());
		};
		assert_eq!(quota.code(), "quota_exceeded");
		assert!(!quota.retryable());
		let Value::Object(details) = quota.details() else {
			panic!("expected quota details object");
		};
		assert_eq!(details.get("database"), Some(&Value::String("app".to_owned())));
		assert_eq!(details.get("generation"), Some(&Value::Number(1.into())));

		let wire = mapped.clone().into_value();
		let decoded = TypesError::from_value(wire).unwrap();
		assert_eq!(decoded, mapped);
	}

	#[test]
	fn quota_lifecycle_errors_have_stable_codes_and_retryability() {
		assert_quota_error(
			Error::QuotaAlreadyExists {
				database: "app".to_owned(),
			},
			"quota_policy_exists",
			false,
		);
		assert_quota_error(
			Error::QuotaNotFound {
				database: "app".to_owned(),
			},
			"quota_policy_not_found",
			false,
		);
		assert_quota_error(
			Error::QuotaGenerationMismatch {
				database: "app".to_owned(),
				expected: 1,
				actual: 2,
			},
			"quota_generation_mismatch",
			false,
		);
		assert_quota_error(
			Error::QuotaRuleNotFound {
				id: "records".to_owned(),
			},
			"quota_rule_not_found",
			false,
		);
		assert_quota_error(
			Error::QuotaPolicyInvalid {
				reason: "bad selector".to_owned(),
			},
			"quota_policy_invalid",
			false,
		);
		assert_quota_error(Error::QuotaConflict, "quota_conflict", true);
		assert_quota_error(
			Error::QuotaPolicyChanged {
				database: "app".to_owned(),
				expected: 1,
				actual: 2,
			},
			"quota_policy_changed",
			true,
		);
	}

	#[test]
	fn quota_ledger_retryability_depends_on_safe_state() {
		let rebuilding = assert_quota_error(
			Error::QuotaUsageNotReady {
				state: "rebuilding".to_owned(),
			},
			"quota_ledger_unavailable",
			true,
		);
		let corrupt = assert_quota_error(
			Error::QuotaUsageInvalid {
				reason: "secret internal key".to_owned(),
			},
			"quota_ledger_unavailable",
			false,
		);

		assert_eq!(
			rebuilding.quota_details().unwrap().details(),
			&Value::Object(surrealdb_types::object! {
				state: "rebuilding",
			})
		);
		assert_eq!(
			corrupt.quota_details().unwrap().details(),
			&Value::Object(surrealdb_types::object! {
				state: "corrupt",
			})
		);
	}
}
