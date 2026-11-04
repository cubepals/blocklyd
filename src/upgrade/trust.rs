// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Whether a node trusts an upgrade offer: only for a blocklyd release, and never an older one.
//!
//! Every release is signed: release.yml signs its version and its binary's sha256 with an Ed25519
//! key only that workflow holds, and publishes the signature as `blocklyd.sig`, among the
//! release's files and beside the binary in the image. A control plane passes it on in its offer
//! but can't make one, so neither it nor its fleet CA can have hosts run a blocklyd of their own.
//! A node also never goes back to an older blocklyd, so an old release, signed in its day, can't be
//! offered to undo a fix. The checks run before anything is downloaded, so a binary that fails
//! them is never run, not even for its `--version`.
//!
//! What is signed is three lines, as release.yml writes them:
//!
//! ```text
//! blocklyd release
//! <version>
//! <sha256, lowercase hex>
//! ```
//!
//! The keys are the PEM blocks in `deploy/release-keys.pem`, compiled in. A new key is added there
//! a release before release.yml signs with it, and the old one goes a release after.

use rustls_pki_types::SubjectPublicKeyInfoDer;
use rustls_pki_types::pem::PemObject;

use crate::protocol::wire::UpgradeOffer;

/// The public keys a release may be signed by, as `openssl pkey -pubout` writes them.
const RELEASE_KEYS: &str = include_str!("../../deploy/release-keys.pem");
const KEY_BYTES: usize = 32;
/// What an Ed25519 SubjectPublicKeyInfo holds before the key's 32 bytes: a SEQUENCE of the
/// algorithm, id-Ed25519 (1.3.101.112), and a BIT STRING with no unused bits (RFC 8410 §4).
const ED25519_SPKI_PREFIX: [u8; 12] = [0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00];

/// The keys a node takes a release's signature from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReleaseKeys(Vec<[u8; KEY_BYTES]>);

impl ReleaseKeys {
    /// The release keys compiled in; none, so that no upgrade is taken, if the file holds none
    /// (`the_release_keys_are_ed25519_keys` keeps that from being released). A debug build also
    /// takes the PEM file BLOCKLYD_TEST_RELEASE_KEY names, so an end-to-end test can sign the
    /// blocklyd it serves; a release build never does.
    pub(crate) fn ours() -> Self {
        let mut keys = Self::from_pem(RELEASE_KEYS.as_bytes()).unwrap_or_else(|e| {
            tracing::error!(error = %e, "deploy/release-keys.pem holds no release key, so no upgrade is taken");
            Self(Vec::new())
        });
        #[cfg(debug_assertions)]
        if let Some(path) = std::env::var_os("BLOCKLYD_TEST_RELEASE_KEY").filter(|p| !p.is_empty()) {
            let path = std::path::PathBuf::from(path);
            match std::fs::read(&path).map_err(|e| e.to_string()).and_then(|pem| Self::from_pem(&pem)) {
                Ok(test) => keys.0.extend(test.0),
                Err(e) => {
                    tracing::warn!(error = %e, path = %path.display(), "BLOCKLYD_TEST_RELEASE_KEY taken as no key")
                }
            }
        }
        keys
    }

    /// Every PUBLIC KEY in `pem`, each of which must be an Ed25519 one.
    pub(crate) fn from_pem(pem: &[u8]) -> Result<Self, String> {
        let mut keys = Vec::new();
        for spki in SubjectPublicKeyInfoDer::pem_slice_iter(pem) {
            let spki = spki.map_err(|e| format!("not a PEM PUBLIC KEY: {e}"))?;
            let key = spki
                .strip_prefix(&ED25519_SPKI_PREFIX)
                .and_then(|key| <[u8; KEY_BYTES]>::try_from(key).ok())
                .ok_or("a PUBLIC KEY that isn't an Ed25519 one")?;
            keys.push(key);
        }
        if keys.is_empty() {
            return Err("no PUBLIC KEY".into());
        }
        Ok(Self(keys))
    }

    /// Whether `signature`, in hex, is one of these keys' over `statement`.
    fn signed(&self, statement: &str, signature: &str) -> bool {
        let Ok(signature) = hex::decode(signature) else { return false };
        self.0.iter().any(|key| webpki::ring::ED25519.verify_signature(key, statement.as_bytes(), &signature).is_ok())
    }
}

/// What a release's signature signs (module docs). None for a version or a sha256 that could be
/// read as another pair: a version is one word, a sha256 is 64 hex digits.
pub(crate) fn statement(version: &str, sha256: &str) -> Option<String> {
    let word = !version.is_empty() && !version.chars().any(|c| c.is_whitespace() || c.is_control());
    let digest = sha256.len() == 64 && sha256.bytes().all(|b| b.is_ascii_hexdigit());
    (word && digest).then(|| format!("blocklyd release\n{version}\n{}\n", sha256.to_ascii_lowercase()))
}

/// `MAJOR.MINOR.PATCH`, the only versions blocklyd releases, in the order they come.
fn ordered(version: &str) -> Option<(u64, u64, u64)> {
    let mut parts = version.split('.').map(|part| part.parse::<u64>().ok());
    let found = (parts.next()??, parts.next()??, parts.next()??);
    parts.next().is_none().then_some(found)
}

/// Whether a node that runs `from` installs `offer`: signed by one of `keys`, and not older than
/// `from`. Says why not.
pub(crate) fn check(offer: &UpgradeOffer, from: &str, keys: &ReleaseKeys) -> Result<(), String> {
    let to = &offer.version;
    match (ordered(to), ordered(from)) {
        (Some(offered), Some(running)) if offered < running => {
            return Err(format!("blocklyd {to} is older than the {from} this node runs, and a node never goes back"));
        }
        (Some(_), Some(_)) => {}
        _ => return Err(format!("blocklyd {to} can't be told apart from the {from} this node runs as newer or older")),
    }
    let Some(signature) = offer.signature.as_deref() else {
        return Err(format!("the offer of blocklyd {to} carries no release signature, so it wasn't installed"));
    };
    let Some(statement) = statement(to, &offer.sha256) else {
        return Err(format!("the offer of blocklyd {to} names no sha256, so it wasn't installed"));
    };
    if !keys.signed(&statement, signature) {
        return Err(format!(
            "the offer's signature isn't a blocklyd release key's for {to} with sha256 {}, so it wasn't installed",
            offer.sha256
        ));
    }
    Ok(())
}

/// A release key of a test's own, and the keys that take its signatures: for tests here and in
/// `upgrade.rs`.
#[cfg(test)]
pub(crate) fn test_key() -> (rcgen::KeyPair, ReleaseKeys) {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
    let keys = ReleaseKeys::from_pem(key.public_key_pem().as_bytes()).unwrap();
    (key, keys)
}

/// `key`'s signature of `version` and `sha256`, in hex, as release.yml makes one.
#[cfg(test)]
pub(crate) fn test_signature(key: &rcgen::KeyPair, version: &str, sha256: &str) -> String {
    use rcgen::SigningKey;
    hex::encode(key.sign(statement(version, sha256).unwrap().as_bytes()).unwrap())
}

#[cfg(test)]
mod tests {
    use rcgen::{KeyPair, PKCS_ED25519, PublicKeyData};

    use super::*;

    fn offer(version: &str, sha256: &str, signature: Option<String>) -> UpgradeOffer {
        UpgradeOffer { version: version.into(), sha256: sha256.into(), signature }
    }

    #[test]
    fn the_release_keys_are_ed25519_keys() {
        let compiled = ReleaseKeys::from_pem(RELEASE_KEYS.as_bytes()).unwrap();
        assert!(ReleaseKeys::ours().0.starts_with(&compiled.0));
    }

    #[test]
    fn a_public_key_is_read_from_its_pem_as_openssl_and_rcgen_write_it() {
        let key = KeyPair::generate_for(&PKCS_ED25519).unwrap();
        assert_eq!(key.subject_public_key_info(), [&ED25519_SPKI_PREFIX[..], key.public_key_raw()].concat());
        let next = KeyPair::generate_for(&PKCS_ED25519).unwrap();
        // A file of two, as during a change of keys, with words around them.
        let both = format!("the release key\n{}and the next\n{}", key.public_key_pem(), next.public_key_pem());
        let keys = ReleaseKeys::from_pem(both.as_bytes()).unwrap();
        assert_eq!(keys.0, [key.public_key_raw(), next.public_key_raw()]);
        let other = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
        assert!(ReleaseKeys::from_pem(other.public_key_pem().as_bytes()).unwrap_err().contains("isn't an Ed25519"));
        assert!(ReleaseKeys::from_pem(b"").is_err());
    }

    #[test]
    fn an_offer_is_trusted_only_signed_for_its_version_and_sha256() {
        let (key, keys) = test_key();
        let sha = "ab".repeat(32);
        let signed = test_signature(&key, "0.3.0", &sha);
        check(&offer("0.3.0", &sha, Some(signed.clone())), "0.2.0", &keys).unwrap();
        check(&offer("0.3.0", &sha.to_uppercase(), Some(signed.clone())), "0.2.0", &keys).unwrap();

        let refused = |offered: UpgradeOffer| check(&offered, "0.2.0", &keys).unwrap_err();
        assert!(refused(offer("0.3.0", &sha, None)).contains("no release signature"));
        assert!(refused(offer("0.3.1", &sha, Some(signed.clone()))).contains("isn't a blocklyd release key's"));
        assert!(refused(offer("0.3.0", &"cd".repeat(32), Some(signed.clone()))).contains("isn't"));
        assert!(refused(offer("0.3.0", &sha, Some("zz".into()))).contains("isn't"));
        let (stranger, _) = test_key();
        assert!(refused(offer("0.3.0", &sha, Some(test_signature(&stranger, "0.3.0", &sha)))).contains("isn't"));
        // Lines that would read as another statement are never signed for.
        assert!(refused(offer("0.3.0\nx", &sha, Some(signed.clone()))).contains("can't be told apart"));
        assert!(refused(offer("0.3.0", "ab", Some(signed))).contains("names no sha256"));
    }

    #[test]
    fn a_signature_openssl_makes_as_release_yml_does_is_taken() {
        // `openssl pkeyutl -sign -rawin` of blocklyd 9.9.9's statement, by a key whose private
        // half was thrown away (tests/fixtures/README.md).
        let keys = ReleaseKeys::from_pem(include_bytes!("../../tests/fixtures/release-signature/key.pub")).unwrap();
        let signature = hex::encode(include_bytes!("../../tests/fixtures/release-signature/blocklyd.sig"));
        let sha = "9b4b33bf5e345140064483ae6c85c2165a1746cbebf5987a3c802f36981e7e8b";
        check(&offer("9.9.9", sha, Some(signature.clone())), "0.3.5", &keys).unwrap();
        assert!(check(&offer("9.9.8", sha, Some(signature)), "0.3.5", &keys).is_err());
    }

    #[test]
    fn a_node_never_goes_back_to_an_older_blocklyd() {
        let (key, keys) = test_key();
        let sha = "ab".repeat(32);
        let signed = |version: &str| offer(version, &sha, Some(test_signature(&key, version, &sha)));
        for older in ["0.2.9", "0.1.10", "0.3.4"] {
            let refused = check(&signed(older), "0.3.5", &keys).unwrap_err();
            assert!(refused.contains("never goes back"), "{refused}");
        }
        for newer_or_same in ["0.3.5", "0.3.6", "0.10.0", "1.0.0"] {
            check(&signed(newer_or_same), "0.3.5", &keys).unwrap();
        }
        assert!(check(&signed("0.4.0-rc.1"), "0.3.5", &keys).unwrap_err().contains("can't be told apart"));
        assert!(check(&signed("0.4.0"), "dev", &keys).unwrap_err().contains("can't be told apart"));
    }
}
