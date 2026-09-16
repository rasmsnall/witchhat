//! Regex-heavy cleanup: applying find-and-replace rules to a `Utf8` column.
//!
//! Two ways to get rules: bring your own ([`CleanRule::new`], applied with
//! [`apply_rules`]), or ask for one of witchhat's own named, versioned presets
//! ([`preset`], [`clean_with_preset`]). Only the presets are versioned
//! ([`CleanupVersion`]): a caller's own regex is the caller's own algorithm, and
//! witchhat has no more business versioning it than it does versioning their SQL.
//!
//! **The `regex` crate's dialect is not Spark's (Java's) regex dialect.** A pattern
//! written for `pyspark.sql.functions.regexp_replace`, or copied from a Spark SQL
//! `REGEXP_REPLACE`/`RLIKE` expression, is not guaranteed to compile here, let alone
//! match the same way. Concretely unsupported, not just different: lookahead
//! (`(?=...)`, `(?!...)`) and lookbehind (`(?<=...)`, `(?<!...)`), and backreferences
//! (`\1`, `\k<name>` inside the *pattern* itself, as opposed to `$1` in a
//! *replacement*, which `regex` does support). [`CleanRule::new`] rejects a pattern
//! using either as an [`Error::Config`] at compile time, not a runtime surprise, but the
//! rejection message is `regex`'s own compiler diagnostic, not a witchhat-authored
//! explanation of the Java-versus-Rust gap; this module doc is that explanation.
//! Supported and equivalent in both dialects: character classes, quantifiers
//! (`*`/`+`/`?`/`{m,n}`, greedy and lazy), anchors (`^`/`$`), alternation (`|`),
//! non-capturing/capturing/named groups, and Unicode character properties (`\p{...}`).
//! When a Spark-side pattern relies on lookaround or a backreference, the fix is not a
//! witchhat setting: rewrite the pattern in `regex`-compatible terms (often possible for
//! a fixed-width lookaround) or perform that specific cleanup step in Spark itself
//! before/after the witchhat-handled ones. There is no equivalence test in this crate
//! that checks a given pattern's behavior against Spark's actual regex engine; validate
//! any ported pattern against real Spark output before relying on it, the same way
//! [`crate::equivalence::check_equivalence`] is meant to be used for the rest of a
//! pipeline.

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, StringArray};
use regex::Regex;

use crate::error::{Error, Result};

/// One find-and-replace-all rule: every non-overlapping match of `pattern` in a value
/// is replaced with `replacement`.
#[derive(Debug, Clone)]
pub struct CleanRule {
    pattern: Regex,
    replacement: Arc<str>,
}

impl CleanRule {
    /// Compiles a rule from a regular expression and its replacement text.
    ///
    /// `pattern` follows the `regex` crate's syntax, which is not Spark/Java's: no
    /// lookahead, no lookbehind, no backreferences in the pattern (see this module's
    /// top-level docs for the full explanation and what to do instead). `replacement`
    /// follows the `regex` crate's replacement syntax: `$1`, `${name}` and so on refer
    /// to capture groups, and a literal `$` is written `$$`.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if `pattern` does not compile, including a `pattern` that uses
    /// lookaround or a backreference.
    pub fn new(pattern: &str, replacement: impl Into<Arc<str>>) -> Result<Self> {
        Ok(Self {
            pattern: Regex::new(pattern).map_err(Error::config)?,
            replacement: replacement.into(),
        })
    }
}

/// Applies `rules`, in order, to every non-null value of `input`. A null stays null.
///
/// Each rule's output feeds the next, so `rules = [trim, collapse_whitespace]` trims
/// first and then collapses interior whitespace in the trimmed result, not the other
/// way around.
///
/// # Panics
///
/// Does not panic. Not async; runs on the calling thread in time linear in the total
/// size of `input`'s non-null values times `rules.len()`, with no I/O.
///
/// # Examples
///
/// ```
/// use arrow_array::{Array, StringArray};
/// use witchhat_core::clean::{CleanRule, apply_rules};
///
/// let input = StringArray::from(vec![Some("  a   b  "), None]);
/// let rules = vec![
///     CleanRule::new(r"^\s+|\s+$", "").unwrap(),
///     CleanRule::new(r"\s+", " ").unwrap(),
/// ];
/// let out = apply_rules(&input, &rules);
/// assert_eq!(out.value(0), "a b");
/// assert!(out.is_null(1));
/// ```
pub fn apply_rules(input: &StringArray, rules: &[CleanRule]) -> StringArray {
    let mut builder = StringBuilder::with_capacity(input.len(), 0);
    for i in 0..input.len() {
        if input.is_null(i) {
            builder.append_null();
            continue;
        }
        let mut value = input.value(i).to_string();
        for rule in rules {
            value = rule
                .pattern
                .replace_all(&value, rule.replacement.as_ref())
                .into_owned();
        }
        builder.append_value(&value);
    }
    builder.finish()
}

/// Names one exact, frozen set of built-in [`CleanRule`] presets. See
/// [`HashVersion`](crate::HashVersion) for why witchhat's own named behaviour is
/// versioned; a caller's own rules, passed directly to [`apply_rules`], are not
/// affected by this at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum CleanupVersion {
    /// The first preset set. See [`preset`] for the names it defines.
    #[default]
    V1,
}

impl CleanupVersion {
    /// The version [`preset`] uses when a caller does not pin one.
    pub const CURRENT: CleanupVersion = CleanupVersion::V1;

    /// The version's stable name.
    pub fn as_str(self) -> &'static str {
        match self {
            CleanupVersion::V1 => "v1",
        }
    }

    /// Parses a version's stable name, as produced by [`CleanupVersion::as_str`].
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "v1" => Some(CleanupVersion::V1),
            _ => None,
        }
    }
}

/// Looks up a named, built-in cleanup preset.
///
/// <table>
/// <tr><th><code>name</code></th><th>Effect</th></tr>
/// <tr><td><code>trim_whitespace</code></td><td>Removes leading and trailing whitespace</td></tr>
/// <tr><td><code>collapse_whitespace</code></td><td>Collapses any run of whitespace to a single space</td></tr>
/// <tr><td><code>strip_control_characters</code></td><td>Removes ASCII control characters (<code>0x00</code>-<code>0x1F</code>, <code>0x7F</code>)</td></tr>
/// <tr><td><code>strip_non_alphanumeric</code></td><td>Removes everything except letters, digits and whitespace</td></tr>
/// <tr><td><code>digits_only</code></td><td>Removes everything except <code>0</code>-<code>9</code></td></tr>
/// </table>
///
/// # Errors
///
/// [`Error::Config`] if `name` does not name a preset in `version`.
///
/// # Examples
///
/// ```
/// use witchhat_core::clean::{CleanupVersion, preset};
///
/// let rules = preset("trim_whitespace", CleanupVersion::CURRENT).unwrap();
/// assert_eq!(rules.len(), 1);
/// ```
pub fn preset(name: &str, version: CleanupVersion) -> Result<Vec<CleanRule>> {
    match (version, name) {
        (CleanupVersion::V1, "trim_whitespace") => Ok(vec![CleanRule::new(r"^\s+|\s+$", "")?]),
        (CleanupVersion::V1, "collapse_whitespace") => Ok(vec![CleanRule::new(r"\s+", " ")?]),
        (CleanupVersion::V1, "strip_control_characters") => {
            Ok(vec![CleanRule::new(r"[\x00-\x1F\x7F]", "")?])
        }
        (CleanupVersion::V1, "strip_non_alphanumeric") => {
            Ok(vec![CleanRule::new(r"[^A-Za-z0-9\s]", "")?])
        }
        (CleanupVersion::V1, "digits_only") => Ok(vec![CleanRule::new(r"[^0-9]", "")?]),
        (_, other) => Err(Error::config(format!(
            "unknown cleanup preset {other:?} for version {}",
            version.as_str()
        ))),
    }
}

/// [`preset`] followed by [`apply_rules`], for the common case of using a named
/// preset directly rather than inspecting its rules first.
///
/// # Errors
///
/// [`Error::Config`] if `name` does not name a preset in `version`.
pub fn clean_with_preset(
    input: &StringArray,
    name: &str,
    version: CleanupVersion,
) -> Result<StringArray> {
    let rules = preset(name, version)?;
    Ok(apply_rules(input, &rules))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nulls_pass_through() {
        let input = StringArray::from(vec![None::<&str>]);
        let out = clean_with_preset(&input, "trim_whitespace", CleanupVersion::CURRENT).unwrap();
        assert!(out.is_null(0));
    }

    #[test]
    fn trim_whitespace_preset() {
        let input = StringArray::from(vec!["  hello  "]);
        let out = clean_with_preset(&input, "trim_whitespace", CleanupVersion::CURRENT).unwrap();
        assert_eq!(out.value(0), "hello");
    }

    #[test]
    fn collapse_whitespace_preset() {
        let input = StringArray::from(vec!["a    b\t\tc"]);
        let out =
            clean_with_preset(&input, "collapse_whitespace", CleanupVersion::CURRENT).unwrap();
        assert_eq!(out.value(0), "a b c");
    }

    #[test]
    fn strip_control_characters_preset() {
        let input = StringArray::from(vec!["a\u{0007}b\u{001B}c"]);
        let out =
            clean_with_preset(&input, "strip_control_characters", CleanupVersion::CURRENT).unwrap();
        assert_eq!(out.value(0), "abc");
    }

    #[test]
    fn strip_non_alphanumeric_preset() {
        let input = StringArray::from(vec!["a-b_c!d 1"]);
        let out =
            clean_with_preset(&input, "strip_non_alphanumeric", CleanupVersion::CURRENT).unwrap();
        assert_eq!(out.value(0), "abcd 1");
    }

    #[test]
    fn digits_only_preset() {
        let input = StringArray::from(vec!["+1 (555) 123-4567"]);
        let out = clean_with_preset(&input, "digits_only", CleanupVersion::CURRENT).unwrap();
        assert_eq!(out.value(0), "15551234567");
    }

    #[test]
    fn unknown_preset_errors() {
        let input = StringArray::from(vec!["x"]);
        assert!(clean_with_preset(&input, "no_such_preset", CleanupVersion::CURRENT).is_err());
    }

    #[test]
    fn rules_apply_in_order() {
        let input = StringArray::from(vec!["  a   b  "]);
        let rules = vec![
            CleanRule::new(r"^\s+|\s+$", "").unwrap(),
            CleanRule::new(r"\s+", " ").unwrap(),
        ];
        let out = apply_rules(&input, &rules);
        assert_eq!(out.value(0), "a b");
    }

    #[test]
    fn invalid_pattern_errors() {
        assert!(CleanRule::new("(unclosed", "").is_err());
    }

    #[test]
    fn lookaround_and_backreferences_are_rejected_not_silently_different() {
        // Patterns a Spark/Java regexp_replace caller might reasonably port over, but
        // that this crate's `regex` dialect does not support at all: better a compile
        // error here than a pattern that silently matches something else.
        assert!(CleanRule::new(r"foo(?=bar)", "").is_err()); // lookahead
        assert!(CleanRule::new(r"(?<=foo)bar", "").is_err()); // lookbehind
        assert!(CleanRule::new(r"(\w+)\s\1", "").is_err()); // backreference in pattern
    }
}
