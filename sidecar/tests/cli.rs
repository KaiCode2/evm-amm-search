use std::process::Command;

#[test]
fn version_reports_the_release_package_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_evm-amm-route-sidecar"))
        .arg("--version")
        .output()
        .expect("run sidecar binary");

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("version output is utf8"),
        format!("evm-amm-route-sidecar {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
}
