use std::path::Path;
use std::{fs, iter};

use anyhow::Context;
use clippy_utilities::OverflowArithmetic;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::unistd::{self, Whence};
use tracing::info;

use super::test_util;

pub const BENCH_MOUNT_DIR: &str = "/tmp/datenlord_bench_dir";
pub const S3_BENCH_MOUNT_DIR: &str = "./s3_fuse_bench";
pub const DEFAULT_MOUNT_DIR: &str = "/tmp/datenlord_test_dir";
pub const S3_DEFAULT_MOUNT_DIR: &str = "/tmp/datenlord_test_dir";
pub const FILE_CONTENT: &str = "0123456789ABCDEF";

#[cfg(test)]
fn test_create_file(mount_dir: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::MetadataExt;
    info!("test create file");
    let file_path = Path::new(mount_dir).join("test_create_file_user.txt");
    let file_mode = Mode::from_bits_truncate(0o644);
    let file_fd = fcntl::open(&file_path, OFlag::O_CREAT, file_mode)?;
    unistd::close(file_fd)?;
    info!("try get file metadata");
    let file_metadata = fs::metadata(&file_path)?;
    assert_eq!(file_metadata.mode() & 0o777, 0o644);
    // check the file owner
    let file_owner = file_metadata.uid();
    let create_user_id = unistd::getuid();
    assert_eq!(file_owner, create_user_id.as_raw());
    // check the file group
    let file_group = file_metadata.gid();
    let create_group_id = unistd::getgid();
    assert_eq!(file_group, create_group_id.as_raw());
    // check nlink == 1
    assert_eq!(file_metadata.nlink(), 1);
    fs::remove_file(&file_path)?; // immediate deletion
    Ok(())
}

#[cfg(test)]
fn test_create_directory(mount_dir: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    info!("test create directory");
    let dir_path = Path::new(mount_dir).join("test_create_dir_");
    let dir_mode: u32 = 0o755;
    fs::create_dir_all(&dir_path)?;
    fs::set_permissions(dir_path.clone(), fs::Permissions::from_mode(dir_mode))?;

    let dir_metadata = fs::metadata(&dir_path)?;
    assert_eq!(dir_metadata.mode() & 0o777, dir_mode);

    let dir_owner = dir_metadata.uid();
    let create_user_id = unistd::getuid();
    assert_eq!(dir_owner, create_user_id.as_raw());

    let dir_group = dir_metadata.gid();
    let create_group_id = unistd::getgid();
    assert_eq!(dir_group, create_group_id.as_raw());

    fs::remove_dir(&dir_path)?; // immediate deletion
    Ok(())
}

#[cfg(test)]
fn test_create_symlink(mount_dir: &Path) -> anyhow::Result<()> {
    use std::fs::OpenOptions;
    use std::os::unix::fs::{symlink, MetadataExt, OpenOptionsExt};

    info!("test create symlink");
    let target_path = Path::new(mount_dir).join("target_file.txt");
    let symlink_path = Path::new(mount_dir).join("symlink_to_target.txt");

    // Create target file
    OpenOptions::new()
        .create(true)
        .write(true)
        .mode(0o644)
        .open(&target_path)?;

    // Create symlink
    symlink(&target_path, &symlink_path)?;

    let symlink_metadata = fs::symlink_metadata(&symlink_path)?;
    assert!(symlink_metadata.file_type().is_symlink());
    // Check symlink permissions
    assert_eq!(symlink_metadata.mode() & 0o777, 0o777);

    // Check symlink owner
    let symlink_owner = symlink_metadata.uid();
    let create_user_id = unistd::getuid();
    assert_eq!(symlink_owner, create_user_id.as_raw());

    // Check symlink group owner
    let symlink_group = symlink_metadata.gid();
    let create_group_id = unistd::getgid();
    assert_eq!(symlink_group, create_group_id.as_raw());

    // Check symlink points to the right target
    let resolved_path = fs::read_link(&symlink_path)?;
    assert_eq!(resolved_path, target_path);

    fs::remove_file(&target_path)?;
    fs::remove_file(&symlink_path)?;
    Ok(())
}

#[cfg(test)]
fn test_name_too_long(mount_dir: &Path) -> anyhow::Result<()> {
    use nix::fcntl::open;
    info!("test_name_too_long");

    let file_name = "a".repeat(256);
    // try to create file, dir, symlink with name too long
    // expect to fail with ENAMETOOLONG
    let file_path = Path::new(mount_dir).join(file_name);
    let file_mode = Mode::from_bits_truncate(0o644);

    info!("try to create a file with name too long");
    let result = open(&file_path, OFlag::O_CREAT, file_mode);
    match result {
        Ok(_) => panic!("File creation should have failed with ENAMETOOLONG"),
        Err(nix::Error::ENAMETOOLONG) => {} // expected this error
        Err(e) => return Err(e.into()),
    }

    info!("try to create a dir with name too long");
    let result = fs::create_dir_all(&file_path);
    match result {
        Ok(_) => panic!("Directory creation should have failed with ENAMETOOLONG"),
        Err(ref e) if e.raw_os_error() == Some(libc::ENAMETOOLONG) => {} // expected this error
        Err(e) => return Err(e.into()),
    }

    info!("try to create a symlink with name too long");
    let result = std::os::unix::fs::symlink(mount_dir, &file_path);
    match result {
        Ok(_) => panic!("Symlink creation should have failed with ENAMETOOLONG"),
        Err(ref e) if e.raw_os_error() == Some(libc::ENAMETOOLONG) => {} // expected this error
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

#[cfg(test)]
fn test_file_manipulation_rust_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("file manipulation Rust style");
    let file_path = Path::new(mount_dir).join("tmp.txt");
    fs::write(&file_path, FILE_CONTENT)?;
    let bytes = fs::read(&file_path)?;
    let content = String::from_utf8(bytes)?;
    fs::remove_file(&file_path)?; // immediate deletion

    assert_eq!(content, FILE_CONTENT);
    assert!(
        !file_path.exists(),
        "the file {file_path:?} should have been removed",
    );
    Ok(())
}

#[cfg(test)]
fn test_directory_manipulation_rust_way(mount_dir: &Path) -> anyhow::Result<()> {
    info!("Directory manipulation Rust style");

    // Fixed directory name
    let dir_name = "test_dir";
    let dir_path = Path::new(mount_dir).join(dir_name);

    // Create directory
    fs::create_dir_all(&dir_path)?;

    // Fixed file name
    let file_name = "tmp.txt";
    let file_path = dir_path.join(file_name);

    // Write to file
    fs::write(&file_path, FILE_CONTENT)?;

    // Read from file
    let bytes = fs::read(&file_path)?;
    let content = String::from_utf8(bytes)?;

    // Remove file and directory
    fs::remove_file(&file_path)?;
    fs::remove_dir(&dir_path)?;

    // Assertions
    assert_eq!(content, FILE_CONTENT);
    assert!(!file_path.exists(), "the file should have been removed");
    assert!(!dir_path.exists(), "the directory should have been removed");

    Ok(())
}

#[cfg(test)]
fn test_deferred_deletion(mount_dir: &Path) -> anyhow::Result<()> {
    info!("file deletion deferred");
    let file_path = Path::new(mount_dir).join("test_file.txt");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let file_mode = Mode::from_bits_truncate(0o644);
    let fd = fcntl::open(&file_path, oflags, file_mode)?;
    unistd::unlink(&file_path)?; // deferred deletion

    let repeat_times = 3;
    for _ in 0..repeat_times {
        let write_size = unistd::write(fd, FILE_CONTENT.as_bytes())?;
        assert_eq!(
            write_size,
            FILE_CONTENT.len(),
            "the file write size {} is not the same as the expected size {}",
            write_size,
            FILE_CONTENT.len(),
        );
    }
    unistd::fsync(fd)?;

    let mut buffer: Vec<u8> = iter::repeat(0_u8)
        .take(FILE_CONTENT.len().overflow_mul(repeat_times))
        .collect();
    unistd::lseek(fd, 0, Whence::SeekSet)?;
    let read_size = unistd::read(fd, &mut buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(fd)?;

    assert_eq!(
        read_size,
        FILE_CONTENT.len().overflow_mul(repeat_times),
        "the file read size {} is not the same as the expected size {}",
        read_size,
        FILE_CONTENT.len().overflow_mul(repeat_times),
    );
    let str_content: String = FILE_CONTENT.repeat(repeat_times);
    assert_eq!(
        content, str_content,
        "the file read result is not the same as the expected content",
    );

    assert!(
        !file_path.exists(),
        "the file {file_path:?} should have been removed",
    );
    Ok(())
}

#[cfg(test)]
fn test_rename_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename file");
    let from_dir = Path::new(mount_dir).join("from_dir");
    if from_dir.exists() {
        fs::remove_dir_all(&from_dir)?;
    }
    fs::create_dir_all(&from_dir)?;

    let to_dir = Path::new(&mount_dir).join("to_dir");
    if to_dir.exists() {
        fs::remove_dir_all(&to_dir)?;
    }
    fs::create_dir_all(&to_dir)?;

    let old_file = from_dir.join("old.txt");
    fs::write(&old_file, FILE_CONTENT)?;

    let new_file = to_dir.join("new.txt");

    fs::rename(&old_file, &new_file)?;

    let bytes = fs::read(&new_file)?;
    let content = String::from_utf8(bytes)?;
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );

    assert!(
        !old_file.exists(),
        "the old file {old_file:?} should have been removed",
    );
    assert!(new_file.exists(), "the new file {new_file:?} should exist",);

    // Clean up
    fs::remove_dir_all(&from_dir)?;
    fs::remove_dir_all(&to_dir)?;
    Ok(())
}

#[cfg(test)]
fn test_rename_file_replace(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename file no replace");
    let oflags = OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_RDWR;
    let file_mode = Mode::from_bits_truncate(0o644);

    let old_file = Path::new(mount_dir).join("old.txt");
    let old_fd = fcntl::open(&old_file, oflags, file_mode)?;
    let old_file_write_size = unistd::write(old_fd, FILE_CONTENT.as_bytes())?;
    assert_eq!(
        old_file_write_size,
        FILE_CONTENT.len(),
        "the file write size {} is not the same as the expected size {}",
        old_file_write_size,
        FILE_CONTENT.len(),
    );

    let new_file = Path::new(&mount_dir).join("new.txt");
    let new_fd = fcntl::open(&new_file, oflags, file_mode)?;
    let new_file_write_size = unistd::write(new_fd, FILE_CONTENT.as_bytes())?;
    assert_eq!(
        new_file_write_size,
        FILE_CONTENT.len(),
        "the file write size {} is not the same as the expected size {}",
        new_file_write_size,
        FILE_CONTENT.len(),
    );

    fs::rename(&old_file, &new_file).context("rename replace should not fail")?;

    let mut buffer: Vec<u8> = iter::repeat(0_u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(old_fd, 0, Whence::SeekSet)?;
    let old_file_read_size = unistd::read(old_fd, &mut buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(old_fd)?;
    assert_eq!(
        old_file_read_size,
        FILE_CONTENT.len(),
        "the file read size {} is not the same as the expected size {}",
        old_file_read_size,
        FILE_CONTENT.len(),
    );
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );

    let mut buffer: Vec<u8> = iter::repeat(0_u8).take(FILE_CONTENT.len()).collect();
    unistd::lseek(new_fd, 0, Whence::SeekSet)?;
    let new_file_read_size = unistd::read(new_fd, &mut buffer)?;
    let content = String::from_utf8(buffer)?;
    unistd::close(new_fd)?;
    assert_eq!(
        new_file_read_size,
        FILE_CONTENT.len(),
        "the file read size {} is not the same as the expected size {}",
        new_file_read_size,
        FILE_CONTENT.len(),
    );
    assert_eq!(
        content, FILE_CONTENT,
        "the file read result is not the same as the expected content",
    );

    assert!(
        !old_file.exists(),
        "the old file {new_file:?} should not exist",
    );
    assert!(new_file.exists(), "the new file {new_file:?} should exist",);

    // Clean up
    fs::remove_file(&new_file)?;
    Ok(())
}

#[cfg(test)]
fn test_rename_dir(mount_dir: &Path) -> anyhow::Result<()> {
    info!("rename directory");
    let from_dir = Path::new(mount_dir).join("from_dir");
    if from_dir.exists() {
        fs::remove_dir_all(&from_dir)?;
    }
    fs::create_dir_all(&from_dir)?;

    let to_dir = Path::new(&mount_dir).join("to_dir");
    if to_dir.exists() {
        fs::remove_dir_all(&to_dir)?;
    }
    fs::create_dir_all(&to_dir)?;

    let old_sub_dir = from_dir.join("old_sub");
    fs::create_dir_all(&old_sub_dir)?;
    let new_sub_dir = to_dir.join("new_sub");
    fs::rename(&old_sub_dir, &new_sub_dir)?;

    assert!(
        !old_sub_dir.exists(),
        "the old directory {old_sub_dir:?} should have been removed"
    );
    assert!(
        new_sub_dir.exists(),
        "the new directory {new_sub_dir:?} should exist",
    );

    // Clean up
    fs::remove_dir_all(&from_dir)?;
    fs::remove_dir_all(&to_dir)?;
    Ok(())
}

#[cfg(test)]
fn test_symlink_dir(mount_dir: &Path) -> anyhow::Result<()> {
    info!("create and read symlink to directory");

    let current_dir = std::env::current_dir()?;
    std::env::set_current_dir(Path::new(mount_dir))?;

    let src_dir = Path::new("src_dir");
    if src_dir.exists() {
        fs::remove_dir_all(src_dir)?;
    }
    fs::create_dir_all(src_dir).context(format!("failed to create directory={src_dir:?}"))?;

    let src_file_name = "src.txt";
    let src_path = Path::new(&src_dir).join(src_file_name);

    let dst_dir = Path::new("dst_dir");
    unistd::symlinkat(src_dir, None, dst_dir).context("create symlink failed")?;
    let target_path = fs::read_link(dst_dir).context("read symlink failed ")?;
    assert_eq!(src_dir, target_path, "symlink target path not match");

    let dst_path = Path::new("./dst_dir").join(src_file_name);
    fs::write(&dst_path, FILE_CONTENT).context(format!("failed to write to file={dst_path:?}"))?;
    let content = fs::read_to_string(src_path).context("read symlink target file failed")?;
    assert_eq!(
        content, FILE_CONTENT,
        "symlink target file content not match"
    );

    let md = fs::symlink_metadata(dst_dir).context("read symlink metadata failed")?;
    assert!(
        md.file_type().is_symlink(),
        "file type should be symlink other than {:?}",
        md.file_type(),
    );

    let entries = fs::read_dir(dst_dir)
        .context("ready symlink target directory failed")?
        .filter_map(|e| {
            if let Ok(entry) = e {
                Some(entry.file_name())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 1, "the directory entry number not match");
    assert_eq!(
        std::ffi::OsStr::new(src_file_name),
        entries
            .get(0)
            .unwrap_or_else(|| panic!("failed to get the first entry")),
        "directory entry name not match",
    );

    info!("about to remove src_dir");
    fs::remove_dir_all(src_dir)?; // immediate deletion
    info!("about to remove dst_dir");
    fs::remove_dir_all(dst_dir)?; // immediate deletion
    assert!(!src_dir.exists());
    assert!(!dst_dir.exists());
    std::env::set_current_dir(current_dir)?;
    Ok(())
}

#[cfg(test)]
fn test_symlink_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("create and read symlink to file");

    let current_dir = std::env::current_dir()?;
    std::env::set_current_dir(Path::new(mount_dir))?;

    let src_path = Path::new("src.txt");
    fs::write(src_path, FILE_CONTENT)?;

    let dst_path = Path::new("dst.txt");
    unistd::symlinkat(src_path, None, dst_path).context("create symlink failed")?;
    let target_path = fs::read_link(dst_path).context("read symlink failed ")?;
    assert_eq!(src_path, target_path, "symlink target path not match");

    let content = fs::read_to_string(dst_path).context("read symlink target file failed")?;
    assert_eq!(
        content, FILE_CONTENT,
        "symlink target file content not match"
    );

    let md = fs::symlink_metadata(dst_path).context("read symlink metadata failed")?;
    assert!(
        md.file_type().is_symlink(),
        "file type should be symlink other than {:?}",
        md.file_type(),
    );
    fs::remove_file(src_path)?; // immediate deletion
    fs::remove_file(dst_path)?; // immediate deletion
    assert!(!src_path.exists());
    assert!(!dst_path.exists());
    std::env::set_current_dir(current_dir)?;
    Ok(())
}

/// Test bind mount a FUSE directory to a tmpfs directory
/// this test case need root privilege
#[cfg(target_os = "linux")]
#[cfg(test)]
fn test_bind_mount(fuse_mount_dir: &Path) -> anyhow::Result<()> {
    use nix::mount::MsFlags;

    pub fn cleanup_dir(directory: &Path) -> anyhow::Result<()> {
        let umount_res = nix::mount::umount2(directory, nix::mount::MntFlags::MNT_FORCE);
        if umount_res.is_err() {
            info!("cleanup_dir() failed to un-mount {directory:?}");
        }
        fs::remove_dir_all(directory)
            .context(format!("cleanup_dir() failed to remove {directory:?}"))?;
        Ok(())
    }

    if unistd::geteuid().is_root() {
        info!("test bind mount with root user");
    } else {
        // Skip bind mount test for non-root user
        return Ok(());
    }
    let from_dir = Path::new(fuse_mount_dir).join("bind_from_dir");
    if from_dir.exists() {
        cleanup_dir(&from_dir).context(format!("failed to cleanup {from_dir:?}"))?;
    }
    fs::create_dir_all(&from_dir).context(format!("failed to create {from_dir:?}"))?;

    let target_dir = Path::new("/tmp/bind_target_dir");
    if target_dir.exists() {
        cleanup_dir(target_dir).context(format!("failed to cleanup {target_dir:?}"))?;
    }
    fs::create_dir_all(target_dir).context(format!("failed to create {from_dir:?}"))?;

    nix::mount::mount::<Path, Path, Path, Path>(
        Some(&from_dir),
        target_dir,
        None, // fstype
        MsFlags::MS_BIND | MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOEXEC,
        None, // mount option data
    )
    .context(format!(
        "failed to bind mount {from_dir:?} to {target_dir:?}",
    ))?;

    let file_path = Path::new(&target_dir).join("tmp.txt");
    fs::write(&file_path, FILE_CONTENT)?;
    let content = fs::read_to_string(&file_path)?;
    fs::remove_file(&file_path)?; // immediate deletion
    assert_eq!(content, FILE_CONTENT, "file content not match");

    nix::mount::umount(target_dir).context(format!("failed to un-mount {target_dir:?}"))?;

    fs::remove_dir_all(&from_dir).context(format!("failed to remove {from_dir:?}"))?;
    fs::remove_dir_all(target_dir).context(format!("failed to remove {target_dir:?}"))?;
    Ok(())
}

#[cfg(test)]
fn test_delete_file(mount_dir: &Path) -> anyhow::Result<()> {
    info!("test delete file");
    let file_path = Path::new(mount_dir).join("test_delete_file.txt");
    let file_mode = Mode::from_bits_truncate(0o644);
    let file_fd = fcntl::open(&file_path, OFlag::O_CREAT, file_mode)?;
    unistd::close(file_fd)?;

    fs::remove_file(&file_path)?;

    let result = fs::metadata(&file_path);
    match result {
        Ok(_) => panic!("File deletion failed"),
        Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => {} // expected this error
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_all() -> anyhow::Result<()> {
    run_test().await
}

async fn run_test() -> anyhow::Result<()> {
    _run_test(DEFAULT_MOUNT_DIR, true).await
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn run_s3_test() -> anyhow::Result<()> {
    _run_test(S3_DEFAULT_MOUNT_DIR, false).await
}

async fn _run_test(mount_dir_str: &str, is_s3: bool) -> anyhow::Result<()> {
    info!("begin integration test");
    let mount_dir = Path::new(mount_dir_str);
    let th = test_util::setup(mount_dir, is_s3).await?;

    test_delete_file(mount_dir).context("test_delete_file() failed")?;
    test_file_manipulation_rust_way(mount_dir)
        .context("test_file_manipulation_rust_way() failed")?;
    test_directory_manipulation_rust_way(mount_dir)
        .context("test_directory_manipulation_rust_way() failed")?;
    test_create_file(mount_dir).context("test_create_file() failed")?;
    test_create_directory(mount_dir).context("test_create_directory() failed")?;
    test_create_symlink(mount_dir).context("test_create_symlink() failed")?;
    test_name_too_long(mount_dir).context("test_name_too_long() failed")?;
    test_symlink_dir(mount_dir).context("test_symlink_dir() failed")?;
    test_symlink_file(mount_dir).context("test_symlink_file() failed")?;
    #[cfg(target_os = "linux")]
    test_bind_mount(mount_dir).context("test_bind_mount() failed")?;
    test_deferred_deletion(mount_dir).context("test_deferred_deletion() failed")?;
    test_rename_file_replace(mount_dir).context("test_rename_file_replace() failed")?;
    test_rename_file(mount_dir).context("test_rename_file() failed")?;
    test_rename_dir(mount_dir).context("test_rename_dir() failed")?;

    test_util::teardown(mount_dir, th).await?;

    Ok(())
}

// TODO: check the logic of this benchmark and make it could be run in CI
#[allow(dead_code)]
async fn run_bench() -> anyhow::Result<()> {
    _run_bench(BENCH_MOUNT_DIR, true).await
}

#[ignore]
#[tokio::test(flavor = "multi_thread")]
async fn run_s3_bench() -> anyhow::Result<()> {
    _run_bench(S3_BENCH_MOUNT_DIR, true).await
}

async fn _run_bench(mount_dir_str: &str, is_s3: bool) -> anyhow::Result<()> {
    let mount_dir = Path::new(mount_dir_str);
    let th = test_util::setup(mount_dir, is_s3).await?;

    let fio_handle = std::process::Command::new("fio")
        .arg("./scripts/perf/fio-jobs.ini")
        .env("BENCH_DIR", mount_dir)
        .output()
        .context("fio command failed to start, maybe install fio first")?;
    let fio_res = if fio_handle.status.success() {
        fs::write("bench.log", &fio_handle.stdout).context("failed to write bench log")?;
        Ok(())
    } else {
        let stderr = String::from_utf8_lossy(&fio_handle.stderr);
        info!("fio failed to run, the error is: {}", &stderr);
        Err(anyhow::anyhow!(
            "fio failed to run, the error is: {}",
            &stderr,
        ))
    };

    test_util::teardown(mount_dir, th).await?;
    fio_res
}
