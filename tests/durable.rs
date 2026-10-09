//! `durable::write_atomic` under a tight umask. The umask is the whole process's, so this test has a
//! binary to itself: beside other tests it would narrow the modes of what they make meanwhile.

use std::os::unix::fs::PermissionsExt;

use rustix::fs::Mode;

#[test]
fn a_file_gets_the_mode_asked_for_whatever_the_umask() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.pem");
    let before = rustix::process::umask(Mode::from_raw_mode(0o077));
    let written = blocklyd::durable::write_atomic(&path, b"a certificate", 0o644);
    rustix::process::umask(before);
    written.unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o644);
}
