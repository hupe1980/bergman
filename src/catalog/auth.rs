//! The read client's authentication, delegated to Bergman's token source.
//!
//! `iceberg-catalog-rest` exchanges an `OAuth2` credential once and never
//! renews it (its source carries `TODO: Support automatic token refreshing`).
//! Its `AuthManager` extension point is public, so instead of renewing around
//! the client, Bergman hands it the [`TokenSource`] the commit client already
//! uses: every read asks that source for a token, and the source renews when
//! the one in hand is due.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg_catalog_rest::{AuthManager, AuthSession, HttpClient, HttpRequest};

use crate::commit::TokenSource;

/// An [`AuthManager`] presenting the tokens of one shared [`TokenSource`].
#[derive(Debug, Clone)]
pub(crate) struct SharedTokenAuth {
    tokens: Arc<TokenSource>,
}

impl SharedTokenAuth {
    pub(crate) fn new(tokens: Arc<TokenSource>) -> Self {
        Self { tokens }
    }
}

#[async_trait::async_trait]
impl AuthManager for SharedTokenAuth {
    async fn init_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> iceberg::Result<Box<dyn AuthSession>> {
        Ok(Box::new(self.clone()))
    }

    async fn catalog_session(
        &self,
        _client: &HttpClient,
        _props: &HashMap<String, String>,
    ) -> iceberg::Result<Arc<dyn AuthSession>> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait::async_trait]
impl AuthSession for SharedTokenAuth {
    async fn authenticate(&self, request: &mut HttpRequest) -> iceberg::Result<()> {
        let token = self.tokens.token().await.map_err(|e| {
            iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "could not obtain a catalog token",
            )
            .with_source(e)
        })?;

        // No credential configured means no header — not an empty bearer,
        // which some catalogs reject outright.
        if let Some(token) = token {
            let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::DataInvalid,
                        "the catalog token is not a valid header value",
                    )
                    .with_source(e)
                })?;
            value.set_sensitive(true);
            request
                .headers_mut()
                .insert(reqwest::header::AUTHORIZATION, value);
        }
        Ok(())
    }
}
