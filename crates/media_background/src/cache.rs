use crate::Settings;
use crate::playback::{MediaInfo, Process, YoutubeSession, background_work};
use anyhow::{Context as _, Result, bail, ensure};
use futures::AsyncReadExt as _;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use smol::io::AsyncWriteExt as _;
use std::{
    fs::{self, File, OpenOptions},
    io::Write as _,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Record {
    pub video_id: String,
    pub format_id: String,
    pub info: MediaInfo,
    pub highest_available: u32,
    pub bytes: u64,
}

pub(crate) struct Entry {
    pub record: Record,
    pub path: PathBuf,
    pub complete: AtomicBool,
    pub error: Mutex<Option<String>>,
    lease: Arc<File>,
    download: Mutex<Option<smol::Task<()>>>,
}

impl Entry {
    pub async fn copy_to(&self, mut writer: smol::process::ChildStdin) -> Result<()> {
        let mut reader = smol::fs::File::open(&self.path).await?;
        let mut buffer = vec![0; 64 * 1024];
        loop {
            let count = reader.read(&mut buffer).await?;
            if count != 0 {
                writer.write_all(&buffer[..count]).await?;
            } else if self.complete.load(Ordering::Acquire) {
                break;
            } else if let Some(error) = self.error.lock().clone() {
                bail!("{error}");
            } else {
                super::playback::wall_timer(Duration::from_millis(30)).await;
            }
        }
        writer.close().await?;
        Ok(())
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.download.get_mut().take();
    }
}

#[derive(Clone)]
pub(crate) struct Cache {
    directory: PathBuf,
    budget: u64,
    pub youtube: YoutubeSession,
}

impl Cache {
    pub fn new(directory: PathBuf, budget: u64) -> Self {
        Self {
            directory,
            budget,
            youtube: YoutubeSession::default(),
        }
    }

    pub async fn lookup(&self, video_id: &str, height: u32) -> Result<Option<Arc<Entry>>> {
        let directory = self.directory.clone();
        let video_id = video_id.to_owned();
        background_work(move || {
            fs::create_dir_all(&directory)?;
            let mut records = Vec::new();
            for file in fs::read_dir(&directory)? {
                let path = file?.path();
                if path.extension().is_none_or(|extension| extension != "json") {
                    continue;
                }
                let Ok(bytes) = fs::read(&path) else { continue };
                let Ok(record) = serde_json::from_slice::<Record>(&bytes) else {
                    continue;
                };
                if record.video_id == video_id {
                    records.push((path, record));
                }
            }
            records.sort_by_key(|(_, record)| quality_key(record.info.height, height));
            for (path, record) in records {
                // A cache hit must not prevent an upgrade known to be available online.
                if record.info.height < height && record.info.height < record.highest_available {
                    continue;
                }
                let lease = OpenOptions::new()
                    .create(true)
                    .truncate(false)
                    .read(true)
                    .write(true)
                    .open(path.with_extension("lease"))?;
                if lease.try_lock_shared().is_err() {
                    continue;
                }
                let media = path.with_extension("media");
                if !fs::metadata(&media)
                    .is_ok_and(|metadata| metadata.len() == record.bytes && metadata.len() != 0)
                {
                    continue;
                }
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)?
                    .set_modified(SystemTime::now())?;
                return Ok(Some(Arc::new(Entry {
                    record,
                    path: media,
                    complete: AtomicBool::new(true),
                    error: Mutex::new(None),
                    lease: Arc::new(lease),
                    download: Mutex::new(None),
                })));
            }
            Ok(None)
        })
        .await
    }

    pub async fn download(&self, mut record: Record, settings: &Settings) -> Result<Arc<Entry>> {
        ensure!(
            record
                .format_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte)),
            "Invalid YouTube format identity"
        );
        let stem = format!("{}-{}", record.video_id, record.format_id);
        let directory = self.directory.clone();
        let (path, lease, scratch) = background_work(move || -> Result<_> {
            fs::create_dir_all(&directory)?;
            let path = directory.join(stem).with_extension("media");
            let _guard = lock_cache(&directory)?;
            let lease = OpenOptions::new().create(true).truncate(false).read(true).write(true).open(path.with_extension("lease"))?;
            lease.try_lock().context("This rendition is being downloaded or played by another Zed process; retrying later")?;
            // The completion manifest is written last; an orphaned payload is never a cache hit.
            if path.with_extension("json").exists() { fs::remove_file(path.with_extension("json"))?; }
            File::create(&path)?;
            let scratch = tempfile::Builder::new().prefix("download-").tempdir_in(&directory)?;
            Ok((path, Arc::new(lease), scratch))
        }).await?;
        record.bytes = 0;
        let entry = Arc::new(Entry {
            record: record.clone(),
            path: path.clone(),
            lease,
            complete: AtomicBool::new(false),
            error: Mutex::new(None),
            download: Mutex::new(None),
        });
        let cache = self.clone();
        let mut settings = settings.clone();
        if settings.yt_dlp_path.is_relative() && settings.yt_dlp_path.components().count() > 1 {
            settings.yt_dlp_path = std::path::absolute(&settings.yt_dlp_path)?;
        }
        let weak = Arc::downgrade(&entry);
        let download_lease = entry.lease.clone();
        *entry.download.lock() = Some(smol::spawn(async move {
            let result = async {
                cache
                    .download_payload(&settings, &record, &path, &download_lease, scratch.path())
                    .await?;
                let info = super::playback::probe(&settings, &path).await?;
                ensure!(
                    info.duration > 0.0,
                    "Cached YouTube video has no finite duration"
                );
                record.info = info;
                let path = path.clone();
                let lease = download_lease.clone();
                background_work(move || -> Result<()> {
                    let _lease = lease;
                    record.bytes = fs::metadata(&path)?.len();
                    ensure!(record.bytes != 0, "YouTube download is empty");
                    let temporary = path.with_extension("json.tmp");
                    fs::write(&temporary, serde_json::to_vec(&record)?)?;
                    fs::rename(temporary, path.with_extension("json"))?;
                    Ok(())
                })
                .await?;
                Ok::<_, anyhow::Error>(())
            }
            .await;
            if let Some(entry) = weak.upgrade() {
                match result {
                    Ok(()) => {
                        // Convert the download lease to a shared playback pin.
                        let lease = entry.lease.clone();
                        let directory = cache.directory.clone();
                        match background_work(move || {
                            let _guard = lock_cache(&directory)?;
                            // Windows cannot downgrade an existing exclusive lock in place.
                            lease.unlock()?;
                            lease.lock_shared()?;
                            Ok(())
                        })
                        .await
                        {
                            Ok(()) => entry.complete.store(true, Ordering::Release),
                            Err(error) => {
                                *entry.error.lock() =
                                    Some(format!("Cannot pin video cache: {error}"))
                            }
                        }
                    }
                    Err(error) => *entry.error.lock() = Some(format!("Video cache: {error}")),
                }
            }
        }));
        Ok(entry)
    }

    async fn download_payload(
        &self,
        settings: &Settings,
        record: &Record,
        path: &Path,
        lease: &Arc<File>,
        scratch: &Path,
    ) -> Result<()> {
        loop {
            let (mut command, used_cookies) = self.youtube.command(settings)?;
            // stdout downloads still create --FragN files. GUI launches may inherit a
            // read-only working directory, and concurrent downloads must not share fragments.
            command.current_dir(scratch);
            command
                .args([
                    "--format",
                    &record.format_id,
                    "--output",
                    "-",
                    "--no-part",
                    "--no-progress",
                    "--no-cache-dir",
                    "--abort-on-unavailable-fragments",
                    "--",
                ])
                .arg(format!(
                    "https://www.youtube.com/watch?v={}",
                    record.video_id
                ));
            let mut process = Process::spawn(command)?;
            let mut reader = process.stdout()?;
            let mut buffer = vec![0; 256 * 1024];
            let mut received_bytes = false;
            loop {
                let count = reader.read(&mut buffer).await?;
                if count == 0 {
                    break;
                }
                self.append(path.to_owned(), buffer[..count].to_vec(), lease.clone())
                    .await?;
                received_bytes = true;
            }
            if let Err(error) = process.check_status("YouTube download failed").await {
                if self
                    .youtube
                    .retry_with_cookies(settings, &error, used_cookies)
                {
                    if !received_bytes {
                        continue;
                    }
                    // A progressive decoder may still hold this payload open. Let the
                    // player stop it and acquire a new cache entry before restarting.
                    bail!("{error}\nRetrying the interrupted download with configured cookies");
                }
                return Err(error);
            }
            return Ok(());
        }
    }

    async fn append(&self, path: PathBuf, bytes: Vec<u8>, lease: Arc<File>) -> Result<()> {
        let directory = self.directory.clone();
        let budget = self.budget;
        background_work(move || {
            // A cancelled download must not release its pin while an already-running write can still touch the payload.
            let _lease = lease;
            let _guard = lock_cache(&directory)?;
            reserve(&directory, budget, bytes.len() as u64, &path)?;
            OpenOptions::new()
                .append(true)
                .open(path)?
                .write_all(&bytes)?;
            Ok(())
        })
        .await
    }
}

fn lock_cache(directory: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(directory.join("cache.lock"))?;
    file.lock()?;
    Ok(file)
}

pub(crate) fn quality_key(height: u32, target: u32) -> (bool, u32) {
    if height >= target {
        (false, height)
    } else {
        (true, u32::MAX - height)
    }
}

fn reserve(directory: &Path, budget: u64, incoming: u64, current: &Path) -> Result<()> {
    let mut total = 0u64;
    let mut candidates = Vec::new();
    for file in fs::read_dir(directory)? {
        let file = file?;
        let path = file.path();
        if path
            .extension()
            .is_none_or(|extension| extension != "media")
        {
            continue;
        }
        let metadata = file.metadata()?;
        total = total.saturating_add(metadata.len());
        if path != current {
            let modified = fs::metadata(path.with_extension("json"))
                .and_then(|metadata| metadata.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            candidates.push((modified, path, metadata.len()));
        }
    }
    candidates.sort_by_key(|(modified, _, _)| *modified);
    for (_, path, length) in candidates {
        if total.saturating_add(incoming) <= budget {
            break;
        }
        let lease = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.with_extension("lease"))?;
        if lease.try_lock().is_err() {
            continue;
        }
        fs::remove_file(&path)?;
        if path.with_extension("json").exists() {
            fs::remove_file(path.with_extension("json"))?;
        }
        total = total.saturating_sub(length);
    }
    ensure!(
        total.saturating_add(incoming) <= budget,
        "Background video cache is full; increase background_media.cache_size_mb"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn cookie_fallback_retries_downloads_without_concatenating_partial_payloads() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        smol::block_on(async {
            for partial in [false, true] {
                let directory = tempfile::tempdir()?;
                let extractor = directory.path().join("extractor");
                let probe = directory.path().join("probe");
                fs::write(
                    &extractor,
                    format!(
                        r#"#!/bin/sh
case " $* " in
    *' --cookies '*) echo authenticated >> "$0.calls"; printf complete-video; exit 0;;
esac
echo anonymous >> "$0.calls"
if [ '{partial}' = true ]; then printf partial-video; fi
echo "ERROR: Sign in to confirm you're not a bot. Use --cookies-from-browser or --cookies" >&2
exit 1
"#
                    ),
                )?;
                fs::write(
                    &probe,
                    "#!/bin/sh\nprintf '%s' '{\"streams\":[{\"width\":2,\"height\":2}],\"format\":{\"duration\":\"1.0\"}}'\n",
                )?;
                for executable in [&extractor, &probe] {
                    fs::set_permissions(executable, fs::Permissions::from_mode(0o700))?;
                }
                let settings = Settings {
                    yt_dlp_path: extractor.clone(),
                    ffprobe_path: probe,
                    youtube_cookie_fallback: Some(crate::YoutubeCookieFallback::Cookies {
                        path: directory.path().join("unused cookies.txt"),
                    }),
                    ..Settings::default()
                };
                let cache = Cache::new(directory.path().join("cache"), 1024 * 1024);
                let record = Record {
                    video_id: "abcdefghijk".into(),
                    format_id: "video".into(),
                    info: MediaInfo {
                        width: 2,
                        height: 2,
                        duration: 1.0,
                    },
                    highest_available: 2,
                    bytes: 0,
                };
                let mut entry = cache.download(record.clone(), &settings).await?;
                let first = wait_for_download(&entry).await;
                if partial {
                    assert!(
                        first
                            .expect_err("Partial video must be restarted")
                            .to_string()
                            .contains("Retrying the interrupted download with configured cookies")
                    );
                    assert_eq!(fs::read(&entry.path)?, b"partial-video");
                    assert!(cache.lookup("abcdefghijk", 2).await?.is_none());
                    drop(entry);
                    entry = cache.download(record, &settings).await?;
                    wait_for_download(&entry).await?;
                } else {
                    first?;
                }
                assert_eq!(fs::read(&entry.path)?, b"complete-video");
                let cached = cache
                    .lookup("abcdefghijk", 2)
                    .await?
                    .context("Authenticated video was not cached")?;
                assert_eq!(fs::read(&cached.path)?, b"complete-video");
                assert_eq!(
                    fs::read_to_string(extractor.with_extension("calls"))?,
                    "anonymous\nauthenticated\n"
                );
            }
            Ok(())
        })
    }

    #[cfg(unix)]
    async fn wait_for_download(entry: &Entry) -> Result<()> {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while !entry.complete.load(Ordering::Acquire) {
            if let Some(error) = entry.error.lock().clone() {
                bail!("{error}");
            }
            ensure!(
                std::time::Instant::now() < deadline,
                "Download did not complete"
            );
            super::super::playback::wall_timer(Duration::from_millis(10)).await;
        }
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn downloads_use_separate_writable_scratch_directories() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            let extractor = directory.path().join("extractor");
            let probe = directory.path().join("probe");
            fs::write(
                &extractor,
                "#!/bin/sh\nset -eu\ncase \" $* \" in *' --abort-on-unavailable-fragments '*) ;; *) exit 1;; esac\ntest ! -e ./--Frag1\nprintf fragment > ./--Frag1\nsleep 0.1\npwd\n",
            )?;
            fs::write(
                &probe,
                "#!/bin/sh\nprintf '%s' '{\"streams\":[{\"width\":2,\"height\":2}],\"format\":{\"duration\":\"1.0\"}}'\n",
            )?;
            for executable in [&extractor, &probe] {
                fs::set_permissions(executable, fs::Permissions::from_mode(0o700))?;
            }
            let settings = Settings {
                yt_dlp_path: extractor,
                ffprobe_path: probe,
                ..Settings::default()
            };
            let cache = Cache::new(directory.path().join("cache"), 1024 * 1024);
            let record = |format_id: &str| Record {
                video_id: "abcdefghijk".into(),
                format_id: format_id.into(),
                info: MediaInfo {
                    width: 2,
                    height: 2,
                    duration: 1.0,
                },
                highest_available: 2,
                bytes: 0,
            };
            let (first, second) = futures::try_join!(
                cache.download(record("first"), &settings),
                cache.download(record("second"), &settings),
            )?;
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !first.complete.load(Ordering::Acquire)
                || !second.complete.load(Ordering::Acquire)
            {
                for entry in [&first, &second] {
                    if let Some(error) = entry.error.lock().clone() {
                        bail!("{error}");
                    }
                }
                ensure!(
                    std::time::Instant::now() < deadline,
                    "Downloads did not complete"
                );
                super::super::playback::wall_timer(Duration::from_millis(10)).await;
            }
            let first_scratch = PathBuf::from(fs::read_to_string(&first.path)?.trim());
            let second_scratch = PathBuf::from(fs::read_to_string(&second.path)?.trim());
            assert_ne!(first_scratch, second_scratch);
            let cache_directory = fs::canonicalize(&cache.directory)?;
            for scratch in [&first_scratch, &second_scratch] {
                assert_eq!(scratch.parent(), Some(cache_directory.as_path()));
            }
            while first_scratch.exists() || second_scratch.exists() {
                ensure!(
                    std::time::Instant::now() < deadline,
                    "Scratch directories were not cleaned up"
                );
                super::super::playback::wall_timer(Duration::from_millis(10)).await;
            }
            Ok(())
        })
    }

    #[test]
    fn complete_cache_is_reusable_without_network_and_partial_files_are_not() -> Result<()> {
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            let cache = Cache::new(directory.path().to_owned(), 1024);
            let path = directory.path().join("abcdefghijk-137.media");
            fs::write(&path, [0; 10])?;
            assert!(cache.lookup("abcdefghijk", 720).await?.is_none());
            let record = Record {
                video_id: "abcdefghijk".into(),
                format_id: "137".into(),
                info: MediaInfo {
                    width: 1920,
                    height: 1080,
                    duration: 4.0,
                },
                highest_available: 1080,
                bytes: 10,
            };
            fs::write(path.with_extension("json"), serde_json::to_vec(&record)?)?;
            let entry = cache
                .lookup("abcdefghijk", 720)
                .await?
                .context("Completed cache missed")?;
            assert!(entry.complete.load(Ordering::Acquire));
            assert_eq!(entry.path, path);
            drop(entry);
            fs::write(&path, [0; 5])?;
            assert!(cache.lookup("abcdefghijk", 720).await?.is_none());
            Ok(())
        })
    }

    #[test]
    fn chooses_smallest_sufficient_rendition() {
        let mut heights = [2160, 360, 1080, 720];
        heights.sort_by_key(|height| quality_key(*height, 900));
        assert_eq!(heights, [1080, 2160, 720, 360]);
    }

    #[test]
    fn eviction_preserves_pinned_and_current_payloads() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let current = directory.path().join("current.media");
        let pinned = directory.path().join("pinned.media");
        let old = directory.path().join("old.media");
        fs::write(&current, [0; 4])?;
        fs::write(&pinned, [0; 4])?;
        fs::write(&old, [0; 4])?;
        let lease = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(pinned.with_extension("lease"))?;
        lease.lock_shared()?;
        reserve(directory.path(), 12, 4, &current)?;
        assert!(current.exists() && pinned.exists());
        assert!(!old.exists());
        assert!(reserve(directory.path(), 8, 4, &current).is_err());
        Ok(())
    }
}
