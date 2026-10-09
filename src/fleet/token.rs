//! The enrollment token as an operator pastes it: `bk1.` and base64url JSON naming where the node
//! endpoint is, the fleet CA by its sha256, the deployment, and the one-time secret.
//!
//! ```text
//! bk1.<base64url({"u": "https://10.0.0.2:8443", "h": "<sha256 hex of the CA PEM>", "d": "production", "s": "<secret>"})>
//! ```
//!
//! A token that re-enrolls a node under its own id also names it, as `"n"`, so `blocklyd join`
//! can tell a re-enrollment of this host from a token meant for another.
//!
//! Only the secret is sent to the control plane, which looks it up as it does a bare token. The
//! rest is what a host needs to find and trust it before anything else: the CA's hash pins the
//! certificate fetched from `u`. A bare token, the secret alone, still works where a configuration
//! names the endpoint and the CA by hand. The region, labels and the node to re-enroll stay on the
//! control plane's record of the token: `n` only tells the host, and decides nothing there.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::Deserialize;
use sha2::{Digest, Sha256};

pub(crate) const PREFIX: &str = "bk1.";

#[derive(Clone, Debug, PartialEq, Deserialize)]
pub(crate) struct JoinToken {
    /// The node endpoint, `https://host:port`.
    #[serde(rename = "u")]
    pub(crate) url: String,
    /// sha256 of the fleet CA's certificate, as the endpoint serves it at `/fleet/v1/ca.pem`.
    #[serde(rename = "h")]
    pub(crate) ca_sha256: String,
    #[serde(rename = "d")]
    pub(crate) deployment_id: String,
    /// What the control plane looks up: the whole of a bare token.
    #[serde(rename = "s")]
    pub(crate) secret: String,
    /// The node this token re-enrolls, when it re-enrolls one.
    #[serde(rename = "n", default)]
    pub(crate) node: Option<String>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum TokenError {
    #[error("the token doesn't start with {PREFIX}: it is a bare token, which names no control plane")]
    NotJoinToken,
    #[error("the token is cut short or mistyped: {0}")]
    Malformed(String),
}

impl JoinToken {
    pub(crate) fn decode(text: &str) -> Result<Self, TokenError> {
        let body = text.trim().strip_prefix(PREFIX).ok_or(TokenError::NotJoinToken)?;
        let json = URL_SAFE_NO_PAD
            .decode(body.trim_end_matches('='))
            .map_err(|e| TokenError::Malformed(format!("not base64url ({e})")))?;
        let token: JoinToken = serde_json::from_slice(&json).map_err(|e| TokenError::Malformed(e.to_string()))?;
        let hash_ok = token.ca_sha256.len() == 64 && token.ca_sha256.bytes().all(|b| b.is_ascii_hexdigit());
        let unset = token.deployment_id.is_empty() || token.secret.is_empty() || token.node.as_deref() == Some("");
        if !crate::config::fleet_url_ok(&token.url) || !hash_ok || unset {
            return Err(TokenError::Malformed(
                "u is https://host[:port], h is a sha256 in hex, and d, s and any n are set".into(),
            ));
        }
        Ok(token)
    }

    /// Whether `pem` is the CA this token names, byte for byte as the endpoint serves it.
    pub(crate) fn names_ca(&self, pem: &[u8]) -> bool {
        hex::encode(Sha256::digest(pem)).eq_ignore_ascii_case(&self.ca_sha256)
    }
}

/// What a token file holds, as enrollment reads it: a join token, or a bare one.
pub(crate) fn read(text: &str) -> Result<Option<JoinToken>, TokenError> {
    match JoinToken::decode(text) {
        Ok(token) => Ok(Some(token)),
        Err(TokenError::NotJoinToken) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH: &str = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";

    fn encode(json: &str) -> String {
        format!("{PREFIX}{}", URL_SAFE_NO_PAD.encode(json))
    }

    #[test]
    fn a_join_token_carries_the_endpoint_the_ca_the_deployment_and_the_secret() {
        let text = encode(&format!(r#"{{"u":"https://10.0.0.2:8443","h":"{HASH}","d":"production","s":"c2VjcmV0"}}"#));
        let token = JoinToken::decode(&format!("  {text}\n")).unwrap();
        assert_eq!(
            token,
            JoinToken {
                url: "https://10.0.0.2:8443".into(),
                ca_sha256: HASH.into(),
                deployment_id: "production".into(),
                secret: "c2VjcmV0".into(),
                node: None,
            }
        );
        assert!(token.names_ca(b"test"), "sha256(\"test\")");
        assert!(!token.names_ca(b"test\n"));
        assert_eq!(read(&text).unwrap(), Some(token));
    }

    #[test]
    fn a_token_that_re_enrolls_a_node_names_it() {
        let text = encode(&format!(r#"{{"u":"https://a:1","h":"{HASH}","d":"p","s":"x","n":"n1"}}"#));
        assert_eq!(JoinToken::decode(&text).unwrap().node.as_deref(), Some("n1"));
    }

    #[test]
    fn the_endpoint_may_be_an_address_a_name_or_a_path_under_one() {
        for url in ["https://10.0.0.2:8443", "https://[fd00::1]:8443/", "https://fleet.example/base"] {
            let text = encode(&format!(r#"{{"u":"{url}","h":"{HASH}","d":"p","s":"x"}}"#));
            assert_eq!(JoinToken::decode(&text).unwrap().url, url);
        }
    }

    #[test]
    fn a_bare_token_is_not_a_join_token_but_still_reads() {
        assert_eq!(JoinToken::decode("c2VjcmV0"), Err(TokenError::NotJoinToken));
        assert_eq!(JoinToken::decode("bk2.e30"), Err(TokenError::NotJoinToken));
        assert_eq!(read("c2VjcmV0\n").unwrap(), None);
    }

    #[test]
    fn a_damaged_token_is_refused_and_says_so() {
        for (text, says) in [
            (format!("{PREFIX}not base64!"), "base64url"),
            (encode("{\"u\":"), "EOF"),
            (encode("[]"), "expected struct JoinToken"),
            (encode(&format!(r#"{{"u":"https://a:1","h":"{HASH}","d":"p"}}"#)), "missing field `s`"),
            (encode(&format!(r#"{{"u":"http://a:1","h":"{HASH}","d":"p","s":"x"}}"#)), "https://"),
            (encode(&format!(r#"{{"u":"https://","h":"{HASH}","d":"p","s":"x"}}"#)), "https://host[:port]"),
            (encode(&format!(r#"{{"u":"https://a b:1","h":"{HASH}","d":"p","s":"x"}}"#)), "https://host[:port]"),
            (encode(&format!(r#"{{"u":"https://a:1?x=1","h":"{HASH}","d":"p","s":"x"}}"#)), "https://host[:port]"),
            (encode(r#"{"u":"https://a:1","h":"abc","d":"p","s":"x"}"#), "sha256"),
            (encode(&format!(r#"{{"u":"https://a:1","h":"{HASH}","d":"","s":"x"}}"#)), "d, s and any n"),
            (encode(&format!(r#"{{"u":"https://a:1","h":"{HASH}","d":"p","s":"x","n":""}}"#)), "any n"),
        ] {
            let refused = JoinToken::decode(&text).unwrap_err();
            assert!(matches!(refused, TokenError::Malformed(_)), "{text}: {refused}");
            assert!(refused.to_string().contains(says), "{text}: {refused}");
            assert!(read(&text).is_err());
        }
    }
}
