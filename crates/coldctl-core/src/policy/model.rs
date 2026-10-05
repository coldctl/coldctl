use crate::error::Error;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyConfig {
    pub schema: String,
    pub table: String,
    pub time_column: String,
    pub older_than_days: i32,
    pub batch_size: i32,
    pub equals_column: Option<String>,
    pub equals_value: Option<String>,
}

impl PolicyConfig {
    pub fn validate(&self) -> Result<(), Error> {
        for value in [&self.schema, &self.table, &self.time_column] {
            if value.is_empty() || value.len() > 63 || value.chars().any(char::is_control) {
                return Err(Error::Archive(
                    "schema/table/column identifiers must contain 1–63 bytes without control characters",
                ));
            }
        }
        if !(1..=365000).contains(&self.older_than_days) {
            return Err(Error::Archive("older-than-days must be 1–365000"));
        }
        if !(1..=1000).contains(&self.batch_size) {
            return Err(Error::Archive("batch-size must be 1–1000"));
        }
        match (&self.equals_column, &self.equals_value) {
            (None, None) => {}
            (Some(column), Some(value))
                if !column.is_empty()
                    && column.len() <= 63
                    && !column.chars().any(char::is_control)
                    && value.len() <= 4096 => {}
            _ => {
                return Err(Error::Archive(
                    "provide both equals-column and equals-value; value limit is 4096 bytes",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    pub id: String,
    pub name: String,
    pub source: String,
    pub destination: String,
    pub config: PolicyConfig,
    pub created_at: String,
}
