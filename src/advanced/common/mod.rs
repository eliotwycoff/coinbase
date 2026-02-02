pub mod authentication;

use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

pub struct Error {
    source: Option<Box<dyn std::error::Error + 'static + Send + Sync>>,
    description: String,
    context: Vec<&'static str>,
}

impl Error {
    /// Construct an error variant for failures from a separate domain,
    /// e.g. the database.
    ///
    /// *Usage:* this error wraps errors from other domains.
    pub fn domain(error: Box<dyn std::error::Error + 'static + Send + Sync>) -> Self {
        let description = format!("Domain error => {error}");

        Self {
            source: Some(error),
            description,
            context: vec![],
        }
    }

    /// Construct an error variant for invalid externally supplied data.
    pub fn invalid(data: impl Debug) -> Self {
        Self {
            source: None,
            description: format!("Invalid data => {data:?}"),
            context: vec![],
        }
    }

    /// Construct an error for failures that should normally be possible.
    ///
    /// *Usage:* this error should be restricted to situations that are not
    /// expected to occur; its occurrence indicates a design failure.
    pub fn impossible(hint: impl Debug) -> Self {
        Self {
            source: None,
            description: format!("Impossible failure => {hint:?}"),
            context: vec![],
        }
    }

    /// Construct an error variant for required data that is missing.
    ///
    /// *Usage:* this error suggests that business logic could not
    /// proceed because some necessary piece of data was not found.
    pub fn unavailable(data: impl Debug) -> Self {
        Self {
            source: None,
            description: format!("Data not available => {data:?}"),
            context: vec![],
        }
    }

    /// Construct an error variant for variants that aren't supported yet.
    ///
    /// *Usage:* this error indicates that a feature or scenario is not
    /// yet or will not be implemented.
    pub fn unsupported(variant: impl Into<&'static str>) -> Self {
        let variant: &'static str = variant.into();

        Self {
            source: None,
            description: format!("Not supported => {variant}"),
            context: vec![],
        }
    }

    /// Add the source of the error to the stack trace.
    pub fn with_source(
        mut self,
        source: Box<dyn std::error::Error + 'static + Send + Sync>,
    ) -> Self {
        self.source = Some(source);

        self
    }

    /// Add a contextual clue to the error.
    pub fn with_context(mut self, context: &'static str) -> Self {
        self.context.push(context);

        self
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        // Write all of the contextual clues.
        for (i, context) in self.context.iter().rev().enumerate() {
            if i == 0 {
                write!(f, "{context}")?;
            } else {
                write!(f, " > {context}")?;
            }
        }

        // Write the error description.
        if self.context.len() > 0 {
            write!(f, " > {}", self.description)?;
        } else {
            write!(f, "{}", self.description)?;
        }

        // Write the stack trace, if available.
        let mut error: &(dyn std::error::Error + 'static) = self;

        while let Some(source) = error.source() {
            write!(f, "\n\nCaused by => {source}")?;

            error = source;
        }

        Ok(())
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        <Self as Display>::fmt(self, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|error| error as &dyn std::error::Error)
    }

    fn description(&self) -> &str {
        self.description.as_str()
    }
}
