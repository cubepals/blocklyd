//! `blocklyd dev-certs`: a throwaway CA and certificates for trying blocklyd locally and
//! for tests. Production would issue these from Blockly's own CA (the control plane holds the
//! client key; each host holds only its server key and the CA certificate) with short lifetimes
//! and rotation. See README, "Security model".

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::net::IpAddr;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};

use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, SanType,
};
use time::{Duration, OffsetDateTime};

pub struct DevCerts {
    pub ca_cert: PathBuf,
    pub node_cert: PathBuf,
    pub node_key: PathBuf,
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
}

fn write(path: &Path, contents: &str, mode: u32) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).truncate(true).write(true).mode(mode).open(path)?;
    file.write_all(contents.as_bytes())
}

pub struct Authority {
    issuer: Issuer<'static, KeyPair>,
    pub pem: String,
}

pub fn authority(name: &str) -> anyhow::Result<Authority> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::new(Vec::<String>::new())?;
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign, KeyUsagePurpose::DigitalSignature];
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, name);
    params.distinguished_name = dn;
    params.not_before = OffsetDateTime::now_utc() - Duration::minutes(5);
    params.not_after = OffsetDateTime::now_utc() + Duration::days(30);
    let cert = params.self_signed(&key)?;
    Ok(Authority { pem: cert.pem(), issuer: Issuer::new(params, key) })
}

/// What a leaf certificate authenticates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// A node (serverAuth).
    Server,
    /// A client (clientAuth).
    Client,
}

/// A leaf certificate for `role`.
pub fn leaf(ca: &Authority, dns: &[String], ips: &[IpAddr], role: Role) -> anyhow::Result<(String, String)> {
    let key = KeyPair::generate()?;
    let mut params = CertificateParams::new(dns.to_vec())?;
    for ip in ips {
        params.subject_alt_names.push(SanType::IpAddress(*ip));
    }
    params.extended_key_usages = vec![match role {
        Role::Server => ExtendedKeyUsagePurpose::ServerAuth,
        Role::Client => ExtendedKeyUsagePurpose::ClientAuth,
    }];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, dns.first().cloned().unwrap_or_else(|| "blockly".into()));
    params.distinguished_name = dn;
    params.not_before = OffsetDateTime::now_utc() - Duration::minutes(5);
    params.not_after = OffsetDateTime::now_utc() + Duration::days(30);
    let cert = params.signed_by(&key, &ca.issuer)?;
    Ok((cert.pem(), key.serialize_pem()))
}

/// Writes the CA certificate (never its key), a node certificate, the allowed client's
/// certificate, and one `client-<name>` pair per extra name: identities from the same CA that the
/// node should refuse, for testing exactly that.
pub fn generate(
    out: &Path,
    node_dns: &[String],
    node_ips: &[IpAddr],
    client: &str,
    extra_clients: &[String],
) -> anyhow::Result<DevCerts> {
    fs::create_dir_all(out)?;
    let ca = authority("Blockly fleet dev CA (throwaway)")?;
    for name in extra_clients {
        let (cert, key) = leaf(&ca, std::slice::from_ref(name), &[], Role::Client)?;
        write(&out.join(format!("client-{name}.pem")), &cert, 0o644)?;
        write(&out.join(format!("client-{name}.key")), &key, 0o600)?;
    }
    let (node_cert, node_key) = leaf(&ca, node_dns, node_ips, Role::Server)?;
    let (client_cert, client_key) = leaf(&ca, &[client.to_owned()], &[], Role::Client)?;
    let paths = DevCerts {
        ca_cert: out.join("ca.pem"),
        node_cert: out.join("node.pem"),
        node_key: out.join("node.key"),
        client_cert: out.join("client.pem"),
        client_key: out.join("client.key"),
    };
    write(&paths.ca_cert, &ca.pem, 0o644)?;
    write(&paths.node_cert, &node_cert, 0o644)?;
    write(&paths.node_key, &node_key, 0o600)?;
    write(&paths.client_cert, &client_cert, 0o644)?;
    write(&paths.client_key, &client_key, 0o600)?;
    Ok(paths)
}
