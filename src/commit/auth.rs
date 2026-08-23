//! Authenticating to a catalog, for the commit path.
//!
//! Bergman reads through `iceberg-catalog-rest` and commits through its own
//! client (see [`super`]), which means two clients must authenticate the same
//! way. When they do not, the failure is unusually nasty: reads succeed, the
//! plan looks perfect, and every commit returns 401 — so the tool appears to
//! work and quietly changes nothing.
//!
//! This module therefore speaks the same property vocabulary the catalog client
//! does — `token`, `credential`, `oauth2-server-uri`, `scope` — so one
//! configuration authenticates both.
//!
//! It also **refreshes**, which the catalog client does not (its source carries
//! a `TODO: Support automatic token refreshing`). A one-shot `bergman run`
//! never notices; a daemon holding a one-hour token notices an hour in, when
//! every commit starts failing and reads keep working.

use std::collections::HashMap;
use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::error::{Error, Result};

/// How long before expiry a token is renewed.
///
/// A token fetched with one second left is a token that expires between being
/// fetched and being used. A minute covers a slow commit and any clock skew
/// between Bergman and the authorization server.
const REFRESH_MARGIN: Duration = Duration::from_secs(60);

/// What a catalog expects Bergman to present.
///
/// [`Debug`] is implemented by hand and **redacts every secret**. The derived
/// one would put a client secret and a bearer token into any `{:?}` — and this
/// type is reachable by `Debug` from [`TokenSource`], `RestCommitter` and
/// therefore [`crate::Bergman`] itself, so a single `tracing::debug!(?bergman)`
/// in an embedder would write the credential to its logs.
#[derive(Clone)]
pub enum Credential {
    /// No authentication at all — a lab catalog, or one behind a mesh.
    None,
    /// A bearer token supplied directly.
    ///
    /// Whatever produced it is responsible for its lifetime, so this one is
    /// never refreshed.
    Static(String),
    /// `OAuth2` client credentials, exchanged for a bearer token and renewed
    /// before it expires.
    ClientCredentials {
        /// Where to POST the exchange.
        token_endpoint: String,
        /// The client identifier.
        client_id: String,
        /// The client secret.
        client_secret: String,
        /// The scope to request.
        scope: String,
    },
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // The *shape* is worth showing — "which credential am I using" is a
            // real question during setup — and the values never are.
            Self::None => f.write_str("Credential::None"),
            Self::Static(_) => f.write_str("Credential::Static(<redacted>)"),
            Self::ClientCredentials {
                token_endpoint,
                client_id,
                scope,
                client_secret: _,
            } => f
                .debug_struct("Credential::ClientCredentials")
                .field("token_endpoint", token_endpoint)
                // An identifier, not a secret, and the field an operator most
                // often needs to check.
                .field("client_id", client_id)
                .field("client_secret", &"<redacted>")
                .field("scope", scope)
                .finish(),
        }
    }
}

impl Credential {
    /// Read a credential out of a catalog's configuration.
    ///
    /// The property names are the ones `iceberg-catalog-rest` reads, because
    /// the two clients must be configured once and authenticate identically.
    ///
    /// `uri` is the catalog endpoint, used to derive the default token endpoint
    /// the REST specification defines (`{uri}/v1/oauth/tokens`) when no
    /// `oauth2-server-uri` is given.
    pub fn from_properties(
        properties: &HashMap<String, String>,
        explicit_token: Option<String>,
        uri: &str,
    ) -> Result<Self> {
        // An explicitly configured token wins: `token_env` is Bergman's own
        // knob and naming it is a deliberate act.
        if let Some(token) = explicit_token.or_else(|| properties.get("token").cloned()) {
            return Ok(Self::Static(token));
        }

        let Some(credential) = properties.get("credential") else {
            return Ok(Self::None);
        };

        // `client_id:client_secret`, as the REST specification and every
        // Iceberg client spell it. A bare value is a secret with no id, which
        // some servers accept.
        let (client_id, client_secret) = match credential.split_once(':') {
            Some((id, secret)) => (id.to_string(), secret.to_string()),
            None => (String::new(), credential.clone()),
        };

        let token_endpoint = properties
            .get("oauth2-server-uri")
            .cloned()
            .unwrap_or_else(|| format!("{}/v1/oauth/tokens", uri.trim_end_matches('/')));

        Ok(Self::ClientCredentials {
            token_endpoint,
            client_id,
            client_secret,
            // `catalog` is the default the REST specification names, and what
            // the catalog client sends when nothing else is configured.
            scope: properties
                .get("scope")
                .cloned()
                .unwrap_or_else(|| "catalog".to_string()),
        })
    }

    /// Whether this credential needs renewing over time.
    pub fn is_refreshable(&self) -> bool {
        matches!(self, Self::ClientCredentials { .. })
    }
}

/// Supplies the bearer token for each request, renewing it when it is due.
#[derive(Debug)]
pub struct TokenSource {
    credential: Credential,
    client: Client,
    /// The token in hand and when it stops being usable.
    ///
    /// A lock rather than a channel: the contended case is several commits
    /// starting at once just after expiry, and the right behaviour there is for
    /// one to fetch while the others wait — which is what a write lock does.
    cached: RwLock<Option<Cached>>,
}

#[derive(Clone)]
struct Cached {
    token: String,
    /// `None` when the server did not say, in which case the token is used
    /// until something rejects it.
    renew_at: Option<std::time::Instant>,
}

/// The subset of an `OAuth2` token response Bergman uses.
#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Seconds. Absent from some servers, which the specification permits.
    #[serde(default)]
    expires_in: Option<u64>,
}

impl TokenSource {
    /// Build a source for a credential.
    pub fn new(credential: Credential, client: Client) -> Self {
        Self {
            credential,
            client,
            cached: RwLock::new(None),
        }
    }

    /// The token to present, fetching or renewing if needed.
    ///
    /// `None` means "send no `Authorization` header", which is different from
    /// an empty token: some catalogs reject `Authorization: Bearer ` outright.
    pub async fn token(&self) -> Result<Option<String>> {
        match &self.credential {
            Credential::None => Ok(None),
            Credential::Static(token) => Ok(Some(token.clone())),
            Credential::ClientCredentials { .. } => self.oauth_token().await.map(Some),
        }
    }

    async fn oauth_token(&self) -> Result<String> {
        if let Some(cached) = self.cached.read().await.as_ref()
            && !cached.is_due()
        {
            return Ok(cached.token.clone());
        }

        // Held across the fetch, so a burst of commits arriving after expiry
        // produces one exchange rather than one per commit.
        let mut slot = self.cached.write().await;
        if let Some(cached) = slot.as_ref()
            && !cached.is_due()
        {
            return Ok(cached.token.clone());
        }

        let fresh = self.exchange().await?;
        let token = fresh.token.clone();
        *slot = Some(fresh);
        Ok(token)
    }

    async fn exchange(&self) -> Result<Cached> {
        let Credential::ClientCredentials {
            token_endpoint,
            client_id,
            client_secret,
            scope,
        } = &self.credential
        else {
            unreachable!("only client credentials are exchanged");
        };

        let form = [
            ("grant_type", "client_credentials"),
            ("client_id", client_id.as_str()),
            ("client_secret", client_secret.as_str()),
            ("scope", scope.as_str()),
        ];

        let response = self
            .client
            .post(token_endpoint)
            .form(&form)
            .send()
            .await
            .map_err(|e| Error::config(format!("{token_endpoint}: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            // The body of a failed token exchange names which of client id,
            // secret and scope the server objected to, and without it the
            // operator is left guessing between three possibilities.
            let body = response.text().await.unwrap_or_default();
            return Err(Error::config(format!(
                "{token_endpoint} refused the client credentials ({status}): {}",
                body.trim()
            )));
        }

        let body: TokenResponse = response.json().await.map_err(|e| {
            Error::config(format!("{token_endpoint} returned no usable token: {e}"))
        })?;

        Ok(Cached {
            token: body.access_token,
            renew_at: body.expires_in.map(|seconds| {
                let lifetime = Duration::from_secs(seconds);
                // A token whose whole life is shorter than the margin is
                // renewed halfway through rather than immediately, which would
                // spin.
                let ahead = lifetime.checked_sub(REFRESH_MARGIN).unwrap_or(lifetime / 2);
                std::time::Instant::now() + ahead
            }),
        })
    }
}

impl std::fmt::Debug for Cached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A live bearer token. Whether one is held, and whether it is due, are
        // the useful facts; its value never is.
        f.debug_struct("Cached")
            .field("token", &"<redacted>")
            .field("due", &self.is_due())
            .finish()
    }
}

impl Cached {
    fn is_due(&self) -> bool {
        match self.renew_at {
            Some(at) => std::time::Instant::now() >= at,
            // The server named no lifetime, so there is nothing to renew
            // against. The token is used until something rejects it.
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn debug_output_never_carries_a_secret() {
        // `Credential` is reachable by `Debug` from `TokenSource`,
        // `RestCommitter` and `Bergman`, so a derived impl would put a client
        // secret into any `tracing::debug!(?bergman)` an embedder writes.
        let credential = Credential::from_properties(
            &props(&[("credential", "svc-bergman:hunter2")]),
            None,
            "https://c",
        )
        .unwrap();

        let rendered = format!("{credential:?}");
        assert!(!rendered.contains("hunter2"), "leaked: {rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
        // The shape and the identifier survive, because "which credential am I
        // using" is a real question during setup.
        assert!(rendered.contains("svc-bergman"), "{rendered}");

        let static_token = Credential::Static("a-bearer-token".into());
        assert!(!format!("{static_token:?}").contains("a-bearer-token"));

        let cached = Cached {
            token: "a-bearer-token".into(),
            renew_at: None,
        };
        assert!(!format!("{cached:?}").contains("a-bearer-token"));

        // And through the type that actually appears in an embedder's logs.
        let source = TokenSource::new(credential, Client::new());
        assert!(!format!("{source:?}").contains("hunter2"));
    }

    #[test]
    fn a_configured_token_is_used_directly() {
        let credential =
            Credential::from_properties(&HashMap::new(), Some("t".into()), "https://c").unwrap();
        assert!(matches!(credential, Credential::Static(ref t) if t == "t"));
        assert!(!credential.is_refreshable());
    }

    #[test]
    fn a_token_property_is_read_when_no_env_var_names_one() {
        // Some deployments put the token straight in `properties`, which the
        // catalog client also accepts. Reading it here keeps both clients
        // authenticating identically — one that authenticated and one that did
        // not would fail only on the first commit, long after startup.
        let credential =
            Credential::from_properties(&props(&[("token", "t")]), None, "https://c").unwrap();
        assert!(matches!(credential, Credential::Static(_)));
    }

    #[test]
    fn client_credentials_split_on_the_colon() {
        let credential = Credential::from_properties(
            &props(&[("credential", "svc-bergman:hunter2")]),
            None,
            "https://c",
        )
        .unwrap();

        match credential {
            Credential::ClientCredentials {
                client_id,
                client_secret,
                scope,
                token_endpoint,
            } => {
                assert_eq!(client_id, "svc-bergman");
                assert_eq!(client_secret, "hunter2");
                // The specification's default, and what the catalog client
                // sends. A different default here would authenticate the two
                // clients differently.
                assert_eq!(scope, "catalog");
                assert_eq!(token_endpoint, "https://c/v1/oauth/tokens");
            }
            other => panic!("expected client credentials, got {other:?}"),
        }
    }

    #[test]
    fn a_secret_without_an_id_is_still_a_credential() {
        let credential = Credential::from_properties(
            &props(&[("credential", "just-a-secret")]),
            None,
            "https://c",
        )
        .unwrap();
        assert!(
            matches!(credential, Credential::ClientCredentials { ref client_id, .. } if client_id.is_empty())
        );
    }

    #[test]
    fn an_explicit_authorization_server_overrides_the_derived_one() {
        let credential = Credential::from_properties(
            &props(&[
                ("credential", "id:secret"),
                ("oauth2-server-uri", "https://idp.example.com/token"),
                ("scope", "lakehouse"),
            ]),
            None,
            "https://c",
        )
        .unwrap();

        match credential {
            Credential::ClientCredentials {
                token_endpoint,
                scope,
                ..
            } => {
                assert_eq!(token_endpoint, "https://idp.example.com/token");
                assert_eq!(scope, "lakehouse");
            }
            other => panic!("expected client credentials, got {other:?}"),
        }
    }

    #[test]
    fn no_credential_at_all_sends_no_header() {
        let credential = Credential::from_properties(&HashMap::new(), None, "https://c").unwrap();
        assert!(matches!(credential, Credential::None));
    }

    #[tokio::test]
    async fn an_unauthenticated_source_yields_no_token() {
        // Not an empty string: some catalogs reject `Authorization: Bearer `
        // outright, so the header has to be absent rather than blank.
        let source = TokenSource::new(Credential::None, Client::new());
        assert_eq!(source.token().await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_static_token_is_returned_unchanged() {
        let source = TokenSource::new(Credential::Static("abc".into()), Client::new());
        assert_eq!(source.token().await.unwrap(), Some("abc".to_string()));
    }

    #[test]
    fn a_token_is_renewed_before_it_expires() {
        // A token fetched with one second left expires between being fetched
        // and being used.
        let fresh = Cached {
            token: "t".into(),
            renew_at: Some(std::time::Instant::now() + Duration::from_secs(300)),
        };
        assert!(!fresh.is_due());

        let stale = Cached {
            token: "t".into(),
            renew_at: Some(std::time::Instant::now() - Duration::from_secs(1)),
        };
        assert!(stale.is_due());
    }

    #[test]
    fn a_token_with_no_stated_lifetime_is_never_renewed() {
        // There is nothing to renew against, so it is used until something
        // rejects it. Guessing a lifetime would re-authenticate on a cadence
        // the server never asked for.
        let cached = Cached {
            token: "t".into(),
            renew_at: None,
        };
        assert!(!cached.is_due());
    }

    #[test]
    fn a_very_short_lived_token_is_renewed_halfway_rather_than_immediately() {
        // Subtracting the margin from a 30-second token would put the renewal
        // in the past and spin on the authorization server.
        let lifetime = Duration::from_secs(30);
        let ahead = lifetime.checked_sub(REFRESH_MARGIN).unwrap_or(lifetime / 2);
        assert_eq!(ahead, Duration::from_secs(15));
    }
}
