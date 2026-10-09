//! `blocklyd doctor` and `check-config` as the unit and scripts run them: their exit status, and
//! the JSON `join` reads. Docker is pointed at a socket that doesn't exist.

use std::path::Path;
use std::process::Command;

fn config(dir: &Path, api_listen: &str) -> std::path::PathBuf {
    let certs = blocklyd::certs::generate(&dir.join("certs"), &["localhost".into()], &[], "control", &[]).unwrap();
    let (uid, gid) = (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw());
    let owner = if uid == 0 { "1000:1000".to_owned() } else { format!("{uid}:{gid}") };
    let path = dir.join(format!("blocklyd-{}.toml", api_listen.replace([':', '.'], "-")));
    std::fs::write(
        &path,
        format!(
            "node_id = \"n1\"\ndeployment_id = \"dev\"\nstate_dir = \"{state}\"\n\
             [api]\nlisten = \"{api_listen}\"\n\
             [api.tls]\ncert = \"{cert}\"\nkey = \"{key}\"\nclient_ca = \"{ca}\"\nallowed_clients = [\"control\"]\n\
             [docker]\nsocket = \"{state}/no-docker.sock\"\n\
             [network]\nport_range = [42900, 42901]\n\
             [workloads]\ndata_owner = \"{owner}\"\n[capacity]\nmin_free_disk_mb = 1\n",
            state = dir.join("state").display(),
            cert = certs.node_cert.display(),
            key = certs.node_key.display(),
            ca = certs.ca_cert.display(),
        ),
    )
    .unwrap();
    path
}

fn blocklyd(args: &[&str], config: &Path) -> (bool, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_blocklyd")).args(args).arg("--config").arg(config).output().unwrap();
    (out.status.success(), String::from_utf8_lossy(&out.stdout).into_owned())
}

#[test]
fn docker_down_is_a_warning_and_blocklyd_still_starts() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path(), "127.0.0.1:7443");
    std::fs::create_dir(dir.path().join("state")).unwrap();

    let (ok, out) = blocklyd(&["check-config"], &config);
    assert!(ok, "{out}");
    assert!(out.contains("is valid"), "{out}");

    let (ok, out) = blocklyd(&["doctor", "--preflight"], &config);
    assert!(ok, "{out}");
    assert!(out.contains("blocklyd starts."), "{out}");

    let (_, out) = blocklyd(&["doctor", "--json"], &config);
    let report: serde_json::Value = serde_json::from_str(&out).unwrap();
    let docker = report["checks"].as_array().unwrap().iter().find(|c| c["name"] == "docker").unwrap();
    assert_eq!(docker["status"], "warn");
    assert!(docker["fix"].as_str().unwrap().starts_with("Start Docker"));
}

#[test]
fn preflight_refuses_an_invalid_config_and_nothing_blocklyd_survives() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir(dir.path().join("state")).unwrap();

    let public = config(dir.path(), "203.0.113.9:7443");
    let (ok, out) = blocklyd(&["doctor"], &public);
    assert!(!ok, "a public api.listen is a failure: {out}");
    let (ok, out) = blocklyd(&["doctor", "--preflight"], &public);
    assert!(ok, "but blocklyd starts with one, as it always has: {out}");
    let (ok, _) = blocklyd(&["check-config"], &public);
    assert!(ok);

    let broken = dir.path().join("broken.toml");
    std::fs::write(&broken, "deployment_id = \"dev\"\n").unwrap();
    for args in [&["check-config"][..], &["doctor", "--preflight"], &["doctor"]] {
        let (ok, out) = blocklyd(args, &broken);
        assert!(!ok, "{args:?}: {out}");
    }
}
