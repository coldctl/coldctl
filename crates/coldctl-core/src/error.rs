use std::fmt;

#[derive(Debug)]
pub struct Error;

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("coldctl core error")
    }
}

impl std::error::Error for Error {}
