//! Native namespace-administered database quotas.
use crate::ast::DefineKind;
use crate::parse::*;
impl<'a, 's> Parser<'a, 's> {
	pub(crate) fn quota_statement(&mut self, tag: Tag, start: u32) -> R<NodeId> {
		let first = self.ast.nodes.len();
		self.expect_kw(Kw::Quota, "QUOTA")?;
		let flags = if self.eat_kw(Kw::If) {
			if tag == Tag::Define {
				self.expect_kw(Kw::Not, "NOT")?;
			}
			self.expect_kw(Kw::Exists, "EXISTS")?;
			1
		} else if tag == Tag::Define && self.eat_kw(Kw::Overwrite) {
			2
		} else {
			0
		};
		self.expect_kw(Kw::On, "ON")?;
		self.expect_kw(Kw::Database, "DATABASE")?;
		let database = self.expr(true)?;
		let generation = if self.eat_kw(Kw::Expect) {
			self.expect_kw(Kw::Generation, "GENERATION")?;
			if tag == Tag::Define && flags != 2 {
				return Err(self.unexpected("EXPECT GENERATION only with DEFINE QUOTA OVERWRITE"));
			}
			self.uint_lit()?
		} else if tag != Tag::Define {
			return Err(self.unexpected("EXPECT GENERATION"));
		} else {
			NONE
		};
		let mark = self.vals.len() as u32;
		loop {
			let clause_start = self.tok.start;
			let clause_first = self.ast.nodes.len();
			if (tag == Tag::Define && self.eat_kw(Kw::Rule))
				|| (tag == Tag::Alter && self.eat_kw(Kw::Set))
			{
				if tag == Tag::Alter {
					self.expect_kw(Kw::Rule, "RULE")?;
				}
				let id = self.name()?;
				self.expect_kw(Kw::For, "FOR")?;
				let resource = if self.eat_kw(Kw::Table) {
					0
				} else if self.eat_kw(Kw::Field) {
					1
				} else if self.eat_kw(Kw::Record) {
					2
				} else {
					return Err(self.unexpected("TABLE, FIELD, or RECORD"));
				};
				self.expect_kw(Kw::Match, "MATCH")?;
				let (selector, regex) = if self.eat_kw(Kw::Exact) {
					(self.name()?, 0)
				} else if self.eat_kw(Kw::Regex) {
					if !self.at(T::Slash) {
						return Err(self.unexpected("a regex"));
					}
					(self.atom(true)?, 1)
				} else {
					return Err(self.unexpected("EXACT or REGEX"));
				};
				self.expect_kw(Kw::Limit, "LIMIT")?;
				let limit = if self.eat_kw(Kw::Unlimited) {
					NONE
				} else {
					self.uint_lit()?
				};
				let rec = self.record(&[id, resource, selector, regex, limit]);
				let c = self.stmt_node(Tag::Clause, 0, rec, 0, clause_start, clause_first)?;
				self.vals.push(c);
			} else if tag == Tag::Alter && self.eat_kw(Kw::Drop) {
				self.expect_kw(Kw::Rule, "RULE")?;
				let exists = if self.eat_kw(Kw::If) {
					self.expect_kw(Kw::Exists, "EXISTS")?;
					1
				} else {
					0
				};
				let id = self.name()?;
				let c = self.stmt_node(Tag::Clause, 1, id, exists, clause_start, clause_first)?;
				self.vals.push(c);
			} else {
				break;
			}
		}
		if tag == Tag::Alter && self.vals.len() == mark as usize {
			return Err(self.unexpected("SET RULE or DROP RULE"));
		}
		let list = self.list_from(mark).0;
		let rec = self.record(&[database, generation, flags, list]);
		self.stmt_node(tag, DefineKind::Quota as u8, rec, 0, start, first)
	}
}

#[cfg(test)]
mod tests {
	#[test]
	fn quota_statements() {
		for sql in [
			"DEFINE QUOTA ON DATABASE app RULE cap FOR TABLE MATCH EXACT person LIMIT 10;",
			"DEFINE QUOTA OVERWRITE ON DATABASE $db EXPECT GENERATION 18446744073709551615 RULE cap FOR RECORD MATCH REGEX /person.*/ LIMIT UNLIMITED;",
			"DEFINE QUOTA IF NOT EXISTS ON DATABASE app;",
			"ALTER QUOTA IF EXISTS ON DATABASE app EXPECT GENERATION 1 SET RULE cap FOR FIELD MATCH EXACT person LIMIT 0 DROP RULE IF EXISTS old;",
			"REMOVE QUOTA IF EXISTS ON DATABASE app EXPECT GENERATION 1;",
			"REBUILD QUOTA IF NEEDED ON DATABASE app;",
			"INFO FOR QUOTA ON DATABASE app STRUCTURE;",
		] {
			let (_, diagnostics) = crate::parse(sql);
			assert!(diagnostics.is_empty(), "{sql}: {diagnostics:?}");
		}
	}

	#[test]
	fn quota_rejects_invalid_clauses() {
		for sql in [
			"DEFINE QUOTA ON DATABASE app EXPECT GENERATION 1;",
			"DEFINE QUOTA IF NOT EXISTS ON DATABASE app EXPECT GENERATION 1;",
			"ALTER QUOTA ON DATABASE app EXPECT GENERATION 1;",
			"ALTER QUOTA ON DATABASE app SET RULE x FOR TABLE MATCH EXACT person LIMIT 1;",
			"REMOVE QUOTA ON DATABASE app;",
			"DEFINE QUOTA ON DATABASE app RULE x FOR TABLE MATCH EXACT person LIMIT -1;",
			"DEFINE QUOTA ON DATABASE app RULE x FOR TABLE MATCH REGEX person LIMIT 1;",
			"REBUILD QUOTA IF EXISTS ON DATABASE app;",
		] {
			let (_, diagnostics) = crate::parse(sql);
			assert!(!diagnostics.is_empty(), "accepted invalid SQL: {sql}");
		}
	}
}
