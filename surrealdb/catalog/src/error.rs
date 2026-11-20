//! Failures raised when a catalog definition is absent or already present.
//!
//! The catalog is the set of definitions the database is described by, so most
//! of its failures are about a definition's presence: a lookup needed one that
//! is not there, or a definition wanted a name that is already taken. Three
//! are about a definition's shape instead - an analyzer a live index still
//! depends on, a method repeated across the `FOR` clauses of one `DEFINE API`,
//! and an index whose stored format predates what this build can read. One
//! more is about a stored definition as a whole: it cannot be read, or the
//! store's feature level does not yet allow a write of it. A last group is about the
//! layouts of a table's row generations: a row names one the table's layouts do not
//! list or this build refuses.

// The mapper below is the only place this layer's failures become public.
// A new variant must make that decision explicitly rather than inheriting
// whatever the last arm happened to be.
#![deny(clippy::wildcard_enum_match_arm)]

use common::{LeafError, internal_todo};
use surrealdb_types::{
	AlreadyExistsError, Error as TypesError, NotFoundError, QuotaError, SerdeWrapper, SurrealValue,
	ValidationError, Value,
};

use crate::layout::{LayoutRefusal, RecordLayoutError};
use crate::stored::EntryError;
use crate::val::TableName;

/// One deterministic quota admission violation.
#[derive(Debug)]
pub struct QuotaViolation {
	pub rule: String,
	pub resource: String,
	pub table: String,
	pub current: u64,
	pub delta: i128,
	pub projected: u64,
	pub limit: u64,
	pub over_by: u64,
}

/// Detailed, boxed payload for an aggregated quota admission failure.
///
/// Keeping this out of the inline [`Error`] representation avoids inflating
/// every error value and the deeply nested async futures which carry it.
#[derive(Debug)]
pub struct QuotaExceededError {
	pub database: String,
	pub generation: u64,
	pub violations: Vec<QuotaViolation>,
	pub truncated: bool,
}

impl std::fmt::Display for QuotaExceededError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Quota exceeded with {} violation(s)", self.violations.len())?;
		for violation in &self.violations {
			write!(
				f,
				"; rule '{}' for {} on table '{}': current {}, delta {}, projected {}, limit {}, \
				 over by {}",
				violation.rule,
				violation.resource,
				violation.table,
				violation.current,
				violation.delta,
				violation.projected,
				violation.limit,
				violation.over_by
			)?;
		}
		if self.truncated {
			f.write_str("; additional violations truncated")?;
		}
		Ok(())
	}
}

/// A failure in the catalog layer.
#[derive(Debug, thiserror::Error)]
pub enum Error {
	/// The requested database quota policy does not exist
	#[error("The quota policy for database '{database}' does not exist")]
	QuotaNotFound {
		database: String,
	},

	/// The requested database quota policy already exists
	#[error("The quota policy for database '{database}' already exists")]
	QuotaAlreadyExists {
		database: String,
	},

	/// A quota policy generation guard did not match
	#[error(
		"The quota policy for database '{database}' has generation {actual}, expected {expected}"
	)]
	QuotaGenerationMismatch {
		database: String,
		expected: u64,
		actual: u64,
	},

	/// A quota policy mutation omitted its required generation guard
	#[error("The quota policy for database '{database}' requires `EXPECT GENERATION`")]
	QuotaGenerationRequired {
		database: String,
	},

	/// A quota rule referenced by an ALTER statement does not exist
	#[error("The quota rule '{id}' does not exist")]
	QuotaRuleNotFound {
		id: String,
	},

	/// A quota policy is invalid
	#[error("Invalid quota policy: {reason}")]
	QuotaPolicyInvalid {
		reason: String,
	},

	/// Quota policy metadata is deliberately excluded from ordinary database imports
	#[error("Quota policy statements are not allowed during database import")]
	QuotaImportNotAllowed,

	/// Quota usage metadata or counters are internally inconsistent
	#[error("Invalid quota usage ledger: {reason}")]
	QuotaUsageInvalid {
		reason: String,
	},

	/// A database is fenced for quota usage maintenance
	#[error("Database quota usage is not writable while its ledger is {state}")]
	QuotaUsageNotReady {
		state: String,
	},

	/// A quota-protected transaction lost a conditional storage race
	#[error("Quota admission conflicted with another transaction")]
	QuotaConflict,

	/// The quota policy changed after a protected transaction observed it
	#[error(
		"The quota policy for database '{database}' changed from generation {expected} to {actual}"
	)]
	QuotaPolicyChanged {
		database: String,
		expected: u64,
		actual: u64,
	},

	/// A protected resource would exceed its configured quota
	#[error("{0}")]
	QuotaExceeded(Box<QuotaExceededError>),

	/// An upstream datastore must pass through the explicit native-quota migrator
	#[error(
		"Datastore storage version {actual} requires explicit migration to the native-quota fork format"
	)]
	ForkStorageMigrationRequired {
		actual: u16,
	},

	/// The fork-required storage marker is missing or incompatible
	#[error("Native-quota fork storage format is incompatible: {reason}")]
	ForkStorageFormatIncompatible {
		reason: String,
	},

	/// The requested namespace does not exist
	#[error("The namespace '{name}' does not exist")]
	NsNotFound {
		name: String,
	},

	/// The requested database does not exist
	#[error("The database '{name}' does not exist")]
	DbNotFound {
		name: String,
	},

	/// The requested event does not exist
	#[error("The event '{name}' does not exist")]
	EvNotFound {
		name: String,
	},

	/// The requested function does not exist
	#[error("The function '{name}' does not exist")]
	FcNotFound {
		name: String,
	},

	/// The requested extension does not exist. `name` is its storage name;
	/// the message names it as SurrealQL addresses it.
	#[error("The extension '{}' does not exist", extension_name(.name))]
	MdNotFound {
		name: String,
	},

	/// The requested field does not exist
	#[error("The field '{name}' does not exist")]
	FdNotFound {
		name: String,
	},

	/// The requested model does not exist
	#[error("The model 'ml::{name}' does not exist")]
	MlNotFound {
		name: String,
	},

	/// The cluster node does not exist
	#[error("The node '{uuid}' does not exist")]
	NdNotFound {
		uuid: String,
	},

	/// The requested param does not exist
	#[error("The param '${name}' does not exist")]
	PaNotFound {
		name: String,
	},

	/// The requested sequence does not exist
	#[error("The sequence '{name}' does not exist")]
	SeqNotFound {
		name: String,
	},

	/// The requested config does not exist
	#[error("The config for {name} does not exist")]
	CgNotFound {
		name: String,
	},

	/// The requested table does not exist
	#[error("The table '{name}' does not exist")]
	TbNotFound {
		name: TableName,
	},

	/// The requested api does not exist
	#[error("The api '{value}' does not exist")]
	ApNotFound {
		value: String,
	},

	/// The requested analyzer does not exist
	#[error("The analyzer '{name}' does not exist")]
	AzNotFound {
		name: String,
	},

	/// The requested bucket does not exist
	#[error("The bucket '{name}' does not exist")]
	BuNotFound {
		name: String,
	},

	/// The requested type does not exist
	#[error("The type '{name}' does not exist")]
	TyNotFound {
		name: String,
	},

	/// The requested index does not exist
	#[error("The index '{name}' does not exist")]
	IxNotFound {
		name: String,
	},

	/// The requested root user does not exist
	#[error("The root user '{name}' does not exist")]
	UserRootNotFound {
		name: String,
	},

	/// The requested namespace user does not exist
	#[error("The user '{name}' does not exist in the namespace '{ns}'")]
	UserNsNotFound {
		name: String,
		ns: String,
	},

	/// The requested database user does not exist
	#[error("The user '{name}' does not exist in the database '{db}'")]
	UserDbNotFound {
		name: String,
		ns: String,
		db: String,
	},

	/// The requested root access method does not exist
	#[error("The root access method '{ac}' does not exist")]
	AccessRootNotFound {
		ac: String,
	},

	/// The requested root access grant does not exist
	#[error("The root access grant '{gr}' does not exist for '{ac}'")]
	AccessGrantRootNotFound {
		ac: String,
		gr: String,
	},

	/// The requested namespace access method does not exist
	#[error("The access method '{ac}' does not exist in the namespace '{ns}'")]
	AccessNsNotFound {
		ac: String,
		ns: String,
	},

	/// The requested namespace access grant does not exist
	#[error("The access grant '{gr}' does not exist for '{ac}' in the namespace '{ns}'")]
	AccessGrantNsNotFound {
		ac: String,
		gr: String,
		ns: String,
	},

	/// The requested database access method does not exist
	#[error("The access method '{ac}' does not exist in the database '{db}'")]
	AccessDbNotFound {
		ac: String,
		ns: String,
		db: String,
	},

	/// The requested database access grant does not exist
	#[error("The access grant '{gr}' does not exist for '{ac}' in the database '{db}'")]
	AccessGrantDbNotFound {
		ac: String,
		gr: String,
		ns: String,
		db: String,
	},

	/// The requested api already exists
	#[error("The api '{value}' already exists")]
	ApAlreadyExists {
		value: String,
	},

	/// The requested analyzer already exists
	#[error("The analyzer '{name}' already exists")]
	AzAlreadyExists {
		name: String,
	},

	/// The requested bucket already exists
	#[error("The bucket '{value}' already exists")]
	BuAlreadyExists {
		value: String,
	},

	/// The requested type already exists
	#[error("The type '{value}' already exists")]
	TyAlreadyExists {
		value: String,
	},

	/// The requested database already exists
	#[error("The database '{name}' already exists")]
	DbAlreadyExists {
		name: String,
	},

	/// The requested event already exists
	#[error("The event '{name}' already exists")]
	EvAlreadyExists {
		name: String,
	},

	/// The requested field already exists
	#[error("The field '{name}' already exists")]
	FdAlreadyExists {
		name: String,
	},

	/// The requested function already exists
	#[error("The function '{name}' already exists")]
	FcAlreadyExists {
		name: String,
	},

	/// The requested extension already exists. `name` is its storage name;
	/// the message names it as SurrealQL addresses it.
	#[error("The extension '{}' already exists", extension_name(.name))]
	MdAlreadyExists {
		name: String,
	},

	/// The requested index already exists
	#[error("The index '{name}' already exists")]
	IxAlreadyExists {
		name: String,
	},

	/// The requested model already exists
	#[error("The model '{name}' already exists")]
	MlAlreadyExists {
		name: String,
	},

	/// The requested namespace already exists
	#[error("The namespace '{name}' already exists")]
	NsAlreadyExists {
		name: String,
	},

	/// The requested param already exists
	#[error("The param '${name}' already exists")]
	PaAlreadyExists {
		name: String,
	},

	/// The requested config already exists
	#[error("The config for {name} already exists")]
	CgAlreadyExists {
		name: String,
	},

	/// The requested sequence already exists
	#[error("The sequence '{name}' already exists")]
	SeqAlreadyExists {
		name: String,
	},

	/// The requested table already exists
	#[error("The table '{name}' already exists")]
	TbAlreadyExists {
		name: String,
	},

	/// The requested user already exists
	#[error("The root user '{name}' already exists")]
	UserRootAlreadyExists {
		name: String,
	},

	/// The requested namespace user already exists
	#[error("The user '{name}' already exists in the namespace '{ns}'")]
	UserNsAlreadyExists {
		name: String,
		ns: String,
	},

	/// The requested database user already exists
	#[error("The user '{name}' already exists in the database '{db}'")]
	UserDbAlreadyExists {
		name: String,
		ns: String,
		db: String,
	},

	/// The requested root access method already exists
	#[error("The root access method '{ac}' already exists")]
	AccessRootAlreadyExists {
		ac: String,
	},

	/// The requested namespace access method already exists
	#[error("The access method '{ac}' already exists in the namespace '{ns}'")]
	AccessNsAlreadyExists {
		ac: String,
		ns: String,
	},

	/// The requested database access method already exists
	#[error("The access method '{ac}' already exists in the database '{db}'")]
	AccessDbAlreadyExists {
		ac: String,
		ns: String,
		db: String,
	},

	/// The analyzer cannot be removed because it is referenced by an index
	#[error(
		"The analyzer '{name}' is in use by index '{index}' on table '{table}' and cannot be removed"
	)]
	AzInUse {
		name: String,
		table: String,
		index: String,
	},

	/// The same method appears in more than one `FOR` clause on a single
	/// `DEFINE API` statement
	#[error("The method '{method}' is defined in more than one FOR clause on api '{value}'")]
	ApMethodDuplicate {
		value: String,
		method: String,
	},

	/// The route path of an API definition failed to parse.
	#[error("The string could not be parsed into a path: {0}")]
	InvalidPath(String),

	/// A generic failure raised while classifying an aggregation.
	#[error("Invalid query: {message}")]
	Query {
		message: String,
	},

	/// The wrong quantity or magnitude of arguments was given for the specified
	/// function
	#[error("Invalid aggregation: {message}")]
	InvalidAggregation {
		message: String,
	},

	#[error(
		"Incorrect selector for aggregate selection, expression `{expr}` within in selector cannot be aggregated in a group."
	)]
	InvalidAggregationSelector {
		expr: String,
	},

	/// A stored definition could not be read, or could not be written at the store's feature
	/// level.
	#[error("{0}")]
	Entry(EntryError),

	/// A record could not be read or written because of its table's layouts.
	#[error("{0}")]
	Layout(RecordLayoutError),

	/// A migration was asked of a store whose feature level does not yet write the stored
	/// grammar: its rows carry no generation stamp.
	#[error(
		"Cannot migrate the rows of table '{table}': the datastore's feature level does not yet store records in the 4.0 format"
	)]
	MigrationNeedsStoredFormat {
		table: String,
	},

	/// A migration was committed under a name its table has already run.
	#[error("The table '{table}' has already run a migration named '{name}'")]
	MigrationAlreadyRun {
		table: String,
		name: String,
	},
}

/// The public kind of a refusal to read or write a stored entry.
///
/// An entry written by a newer release, or a value the store's feature level does not yet allow,
/// is a matter of how the deployment is configured. Every other refusal is a stored value that
/// cannot be read.
fn entry_kind(entry: EntryError, message: String) -> TypesError {
	match entry {
		EntryError::NeedsRelease {
			..
		}
		| EntryError::AboveLevel {
			..
		} => TypesError::configuration(message, None),
		EntryError::NoHeader
		| EntryError::Marker(_)
		| EntryError::ReservedBits(_)
		| EntryError::Floor(_)
		| EntryError::NotARelease(_)
		| EntryError::RevisionTrailing {
			..
		}
		| EntryError::Unlevelled {
			..
		}
		| EntryError::RevisionWrite {
			..
		}
		| EntryError::Body {
			..
		} => TypesError::serialization(message, None),
	}
}

impl LeafError for Error {
	fn map_kind(self, message: String) -> TypesError {
		match self {
			Error::QuotaAlreadyExists {
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
			Error::QuotaNotFound {
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
			Error::QuotaGenerationMismatch {
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
			Error::QuotaGenerationRequired {
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
			Error::QuotaRuleNotFound {
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
			Error::QuotaPolicyInvalid {
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
			Error::QuotaImportNotAllowed => TypesError::quota(
				message,
				QuotaError::new(
					"quota_import_not_allowed",
					false,
					Value::Object(Default::default()),
				),
			),
			Error::QuotaUsageInvalid {
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
			Error::QuotaUsageNotReady {
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
			Error::QuotaConflict => TypesError::quota(
				message,
				QuotaError::new("quota_conflict", true, Value::Object(Default::default())),
			),
			Error::QuotaPolicyChanged {
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
			Error::QuotaExceeded(details) => {
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

			Error::ForkStorageMigrationRequired {
				..
			}
			| Error::ForkStorageFormatIncompatible {
				..
			} => TypesError::configuration(message, None),
			Error::InvalidPath(_)
			| Error::Query {
				..
			}
			| Error::InvalidAggregation {
				..
			}
			| Error::InvalidAggregationSelector {
				..
			} => internal_todo(message),
			Error::Entry(entry) => entry_kind(entry, message),
			// The statement asked for something the store or the table's history refuses: the
			// author can correct it.
			Error::MigrationNeedsStoredFormat {
				..
			}
			| Error::MigrationAlreadyRun {
				..
			} => TypesError::validation(message, ValidationError::InvalidRequest),
			// A generation written by a newer release, or one the store's feature level does not
			// yet allow, is a matter of how the deployment is configured. Every other refusal is
			// a stored value or a set of layouts that cannot be used.
			Error::Layout(layout) => match layout {
				RecordLayoutError::RefusedGeneration {
					cause,
					..
				}
				| RecordLayoutError::CurrentRefused {
					cause,
					..
				}
				| RecordLayoutError::RingRefused {
					cause,
					..
				} => match cause {
					LayoutRefusal::Entry(entry) => entry_kind(entry, message),
					LayoutRefusal::NamesTable {
						..
					} => TypesError::serialization(message, None),
				},
				RecordLayoutError::UnknownGeneration {
					..
				}
				| RecordLayoutError::NoLayouts {
					..
				}
				| RecordLayoutError::ForeignLayouts {
					..
				}
				| RecordLayoutError::Misnumbered {
					..
				}
				| RecordLayoutError::GenerationOutOfRange {
					..
				}
				| RecordLayoutError::OutsideWindow {
					..
				}
				| RecordLayoutError::CurrentMissing {
					..
				}
				| RecordLayoutError::LayoutMismatch {
					..
				}
				| RecordLayoutError::MalformedRow {
					..
				}
				| RecordLayoutError::WriteWithoutLayouts {
					..
				} => TypesError::serialization(message, None),
			},
			// Typed on the wire. Clients branch on these details, so these six
			// arms are the part of this mapper that is a compatibility surface.
			Error::NsNotFound {
				name,
			} => TypesError::not_found(
				message,
				NotFoundError::Namespace {
					name,
				},
			),
			Error::DbNotFound {
				name,
			} => TypesError::not_found(
				message,
				NotFoundError::Database {
					name,
				},
			),
			Error::TbNotFound {
				name,
			} => TypesError::not_found(
				message,
				NotFoundError::Table {
					name: name.into_string(),
				},
			),
			Error::NsAlreadyExists {
				name,
			} => TypesError::already_exists(
				message,
				AlreadyExistsError::Namespace {
					name,
				},
			),
			Error::DbAlreadyExists {
				name,
			} => TypesError::already_exists(
				message,
				AlreadyExistsError::Database {
					name,
				},
			),
			Error::TbAlreadyExists {
				name,
			} => TypesError::already_exists(
				message,
				AlreadyExistsError::Table {
					name,
				},
			),

			// Untyped on the wire. Most of these could carry a `NotFound` or
			// `AlreadyExists` kind and none does, so giving one a kind is a
			// deliberate change to what clients receive rather than something
			// this mapper assumes. A kind chosen from the variant's name would
			// be actively wrong here: it would re-type every `AlreadyExists`
			// below as not-found, along with `AzInUse`, `ApMethodDuplicate` and
			// `AzInUse`, which is neither.
			Error::EvNotFound {
				..
			}
			| Error::FcNotFound {
				..
			}
			| Error::MdNotFound {
				..
			}
			| Error::FdNotFound {
				..
			}
			| Error::MlNotFound {
				..
			}
			| Error::NdNotFound {
				..
			}
			| Error::PaNotFound {
				..
			}
			| Error::SeqNotFound {
				..
			}
			| Error::CgNotFound {
				..
			}
			| Error::ApNotFound {
				..
			}
			| Error::AzNotFound {
				..
			}
			| Error::BuNotFound {
				..
			}
			| Error::TyNotFound {
				..
			}
			| Error::IxNotFound {
				..
			}
			| Error::UserRootNotFound {
				..
			}
			| Error::UserNsNotFound {
				..
			}
			| Error::UserDbNotFound {
				..
			}
			| Error::AccessRootNotFound {
				..
			}
			| Error::AccessGrantRootNotFound {
				..
			}
			| Error::AccessNsNotFound {
				..
			}
			| Error::AccessGrantNsNotFound {
				..
			}
			| Error::AccessDbNotFound {
				..
			}
			| Error::AccessGrantDbNotFound {
				..
			}
			| Error::ApAlreadyExists {
				..
			}
			| Error::AzAlreadyExists {
				..
			}
			| Error::BuAlreadyExists {
				..
			}
			| Error::TyAlreadyExists {
				..
			}
			| Error::EvAlreadyExists {
				..
			}
			| Error::FdAlreadyExists {
				..
			}
			| Error::FcAlreadyExists {
				..
			}
			| Error::MdAlreadyExists {
				..
			}
			| Error::IxAlreadyExists {
				..
			}
			| Error::MlAlreadyExists {
				..
			}
			| Error::PaAlreadyExists {
				..
			}
			| Error::CgAlreadyExists {
				..
			}
			| Error::SeqAlreadyExists {
				..
			}
			| Error::UserRootAlreadyExists {
				..
			}
			| Error::UserNsAlreadyExists {
				..
			}
			| Error::UserDbAlreadyExists {
				..
			}
			| Error::AccessRootAlreadyExists {
				..
			}
			| Error::AccessNsAlreadyExists {
				..
			}
			| Error::AccessDbAlreadyExists {
				..
			}
			| Error::AzInUse {
				..
			}
			| Error::ApMethodDuplicate {
				..
			} => internal_todo(message),
		}
	}
}

/// The name SurrealQL addresses an extension by, from its storage name: a
/// named extension is stored as `mod::name` and addressed as `ext::name`; an
/// unnamed silo definition is stored as `silo::org::pkg::<v>` and addressed
/// as `silo::org::pkg<v>`. Any other name is returned as it is.
pub fn extension_name(storage: &str) -> std::borrow::Cow<'_, str> {
	if let Some(rest) = storage.strip_prefix("mod::") {
		return std::borrow::Cow::Owned(format!("ext::{rest}"));
	}
	if storage.starts_with("silo::")
		&& let Some(at) = storage.find("::<")
	{
		return std::borrow::Cow::Owned(format!("{}{}", &storage[..at], &storage[at + 2..]));
	}
	std::borrow::Cow::Borrowed(storage)
}

impl Error {
	/// Returns true for errors that belong to the native quota wire contract.
	///
	/// Quota admission is finalized during transaction commit. Callers which
	/// normally wrap commit failures as `QueryNotExecuted` use this predicate
	/// to preserve the structured quota kind/details instead.
	pub fn is_native_quota(&self) -> bool {
		matches!(
			self,
			Error::QuotaAlreadyExists { .. }
				| Error::QuotaNotFound { .. }
				| Error::QuotaGenerationMismatch { .. }
				| Error::QuotaGenerationRequired { .. }
				| Error::QuotaRuleNotFound { .. }
				| Error::QuotaPolicyInvalid { .. }
				| Error::QuotaImportNotAllowed
				| Error::QuotaUsageInvalid { .. }
				| Error::QuotaUsageNotReady { .. }
				| Error::QuotaConflict
				| Error::QuotaPolicyChanged { .. }
				| Error::QuotaExceeded(_)
		)
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::{ErrorDetails, SurrealValue, Value};

	use super::*;
	use crate::{QuotaExceededError, QuotaViolation};
	fn into_types_error(error: Error) -> TypesError {
		error.to_types_error()
	}

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
