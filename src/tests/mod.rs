use std::path::PathBuf;

use crate::extract;

#[test]
fn test_roundtrip_small_file() {
    // TODO: this is going to single-thread the tests, fix this before
    // continuing

    const CONTENTS: &str = "hello from file.txt";

    _ = std::fs::remove_dir_all("example");
    std::fs::create_dir_all("example/original").unwrap();
    std::fs::create_dir_all("example/extracted").unwrap();

    std::fs::write("example/original/file.txt", CONTENTS).unwrap();

    std::process::Command::new("borg")
        .arg("init")
        .arg("./example/backup")
        .arg("-e")
        .arg("none")
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    std::process::Command::new("borg")
        .arg("create")
        .arg("./example/backup::{now}")
        .arg("./example/original/file.txt")
        .spawn()
        .unwrap()
        .wait()
        .unwrap();

    extract(PathBuf::from("./example/backup")).unwrap();

    let data = std::fs::read_to_string("example/extracted/example__original__file.txt").unwrap();

    assert_eq!(data, CONTENTS);
}
