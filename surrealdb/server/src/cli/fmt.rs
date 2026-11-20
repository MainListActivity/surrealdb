use anyhow::{Result, bail};
use clap::Args;
use surrealdb_core::syn;
use surrealdb_types::ToSql;
use tokio::fs;
use tokio::io::{self, AsyncReadExt};

#[derive(Args, Debug)]
pub struct FormatCommandArguments {
	#[arg(help = "Path to the SurrealQL file to format. Use dash - to read from stdin.")]
	#[arg(index = 1)]
	file: String,

	#[arg(short, long)]
	#[arg(help = "Overwrite the file with the formatted output instead of printing to stdout.")]
	write: bool,
}

pub async fn init(
	FormatCommandArguments {
		file,
		write,
	}: FormatCommandArguments,
) -> Result<()> {
	let is_stdin = file == "-";

	if write && is_stdin {
		bail!("The --write flag cannot be used when reading from stdin.");
	}

	let content = if is_stdin {
		let mut content = String::new();

		io::stdin().read_to_string(&mut content).await?;

		content
	} else {
		fs::read_to_string(&file).await?
	};

	let formatted = format_source(&content)?;

	if write {
		fs::write(file, formatted).await?
	} else {
		println!("{formatted}");
	}

	Ok(())
}

/// The new parser does not retain comments in its AST. Preserve source that
/// might contain comments rather than silently deleting operator annotations.
/// False positives inside strings are intentionally conservative.
fn format_source(content: &str) -> Result<String> {
	let query = syn::parse(content)?;
	if ["#", "//", "/*", "--"].iter().any(|marker| content.contains(marker)) {
		return Ok(content.to_owned());
	}
	Ok(query.to_sql_pretty())
}

#[cfg(test)]
mod tests {
	use super::format_source;

	#[test]
	fn preserves_comments_without_losing_source() {
		for source in [
			"-- operator note\nRETURN 1;",
			"RETURN /* inline */ 1;",
			"# note\nRETURN 1;",
			"RETURN 1; // trailing\n",
			"RETURN 1; // EOF",
			"# EOF",
			"-- EOF",
			"/* EOF */",
			"",
		] {
			assert_eq!(format_source(source).unwrap(), source);
		}
	}

	#[test]
	fn validates_even_when_preserving_comments() {
		assert!(format_source("# note\nSELECT FROM;").is_err());
	}

	#[test]
	fn formats_comment_free_source() {
		assert_eq!(format_source("return 1;").unwrap(), "RETURN 1;");
	}
}
