mod cache;
mod playback;

use anyhow::{Result, ensure};
use parking_lot::Mutex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Source {
    Image { path: PathBuf },
    Gif { path: PathBuf },
    Video { path: PathBuf },
    YoutubeVideo { url: String },
    YoutubeLive { url: String },
    Holodex { channel_ids: Vec<String> },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Fit {
    #[default]
    Cover,
    Contain,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HardwareAcceleration {
    #[default]
    Auto,
    Off,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum YoutubeCookieFallback {
    /// yt-dlp browser specification, including an optional profile/keyring/container.
    CookiesFromBrowser { browser: String },
    /// Netscape-format cookie file. yt-dlp may update this file.
    Cookies { path: PathBuf },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Settings {
    /// Background source. Null disables playback.
    pub source: Option<Source>,
    /// Opacity of the media, from zero to one.
    pub opacity: f32,
    pub fit: Fit,
    /// Maximum delivered frames per second. Playback time is independent of this limit.
    pub max_fps: u32,
    /// Optional source resolution ceiling. Null follows the window's physical size.
    pub max_height: Option<u32>,
    pub hardware_acceleration: HardwareAcceleration,
    /// Persistent YouTube video cache budget, including partial downloads.
    pub cache_size_mb: u64,
    pub ffmpeg_path: PathBuf,
    pub ffprobe_path: PathBuf,
    pub yt_dlp_path: PathBuf,
    /// Retry YouTube bot-verification failures with these cookies. Null disables authentication.
    pub youtube_cookie_fallback: Option<YoutubeCookieFallback>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            source: None,
            opacity: 0.2,
            fit: Fit::Cover,
            max_fps: 30,
            max_height: None,
            hardware_acceleration: HardwareAcceleration::Auto,
            cache_size_mb: 10 * 1024,
            ffmpeg_path: "ffmpeg".into(),
            ffprobe_path: "ffprobe".into(),
            yt_dlp_path: "yt-dlp".into(),
            youtube_cookie_fallback: None,
        }
    }
}

impl Settings {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.opacity.is_finite() && (0.0..=1.0).contains(&self.opacity),
            "background_media.opacity must be between 0 and 1"
        );
        ensure!(
            (1..=60).contains(&self.max_fps),
            "background_media.max_fps must be between 1 and 60"
        );
        ensure!(
            self.max_height
                .is_none_or(|height| (2..=8192).contains(&height)),
            "background_media.max_height must be between 2 and 8192"
        );
        ensure!(
            (1..=1024 * 1024).contains(&self.cache_size_mb),
            "background_media.cache_size_mb must be between 1 and 1048576"
        );
        if let Some(Source::Holodex { channel_ids }) = &self.source {
            ensure!(
                !channel_ids.is_empty() && channel_ids.len() <= 50,
                "Holodex requires 1 to 50 ordered channel IDs"
            );
            ensure!(
                channel_ids.iter().all(|id| id.len() == 24
                    && id.starts_with("UC")
                    && id
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))),
                "Invalid Holodex YouTube channel ID"
            );
        }
        if let Some(Source::YoutubeVideo { url } | Source::YoutubeLive { url }) = &self.source {
            youtube_id(url)?;
        }
        match &self.youtube_cookie_fallback {
            Some(YoutubeCookieFallback::CookiesFromBrowser { browser }) => ensure!(
                !browser.trim().is_empty() && !browser.contains(['\0', '\r', '\n']),
                "YouTube cookie fallback requires a browser specification"
            ),
            Some(YoutubeCookieFallback::Cookies { path }) => ensure!(
                path.is_absolute() || path.starts_with("~"),
                "YouTube cookie file must be absolute or start with ~/"
            ),
            None => {}
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PixelFormat {
    Nv12,
    Bgra,
}

#[derive(Clone, Debug)]
pub struct Frame {
    pub pipeline: u64,
    pub width: u32,
    pub height: u32,
    pub format: PixelFormat,
    pub data: Arc<[u8]>,
    /// Seconds on the source timeline, never a UI-render counter.
    pub timestamp: f64,
    pub sequence: u64,
}

#[derive(Clone, Default)]
pub struct Snapshot {
    pub frame: Option<Arc<Frame>>,
    pub error: Option<String>,
}

struct Shared {
    presented: std::sync::atomic::AtomicU64,
    snapshot: Mutex<Snapshot>,
    windows: Mutex<BTreeMap<u64, (u32, u32, Instant)>>,
    stopped: AtomicBool,
}

pub struct Player {
    shared: Arc<Shared>,
}

impl Player {
    pub fn new(
        settings: Settings,
        directory: PathBuf,
        http: Arc<dyn http_client::HttpClient>,
    ) -> Result<Self> {
        Self::new_with_holodex_key(
            settings,
            directory,
            http,
            std::env::var("HOLODEX_API_KEY").ok(),
        )
    }

    pub fn new_with_holodex_key(
        settings: Settings,
        directory: PathBuf,
        http: Arc<dyn http_client::HttpClient>,
        holodex_key: Option<String>,
    ) -> Result<Self> {
        settings.validate()?;
        let shared = Arc::new(Shared {
            presented: std::sync::atomic::AtomicU64::new(0),
            snapshot: Mutex::new(Snapshot::default()),
            windows: Mutex::new(BTreeMap::new()),
            stopped: AtomicBool::new(false),
        });
        std::thread::Builder::new()
            .name("background-media".into())
            .spawn({
                let shared = shared.clone();
                move || {
                    playback::lower_thread_priority();
                    if let Err(error) = smol::block_on(playback::run(
                        settings,
                        directory,
                        http,
                        &shared,
                        holodex_key,
                    )) {
                        shared.snapshot.lock().error = Some(format!("Background media: {error}"));
                    }
                }
            })?;
        Ok(Self { shared })
    }

    pub fn snapshot(&self) -> Snapshot {
        self.shared.snapshot.lock().clone()
    }

    pub fn presented(&self, pipeline: u64) {
        self.shared.presented.store(pipeline, Ordering::Release);
    }

    pub fn set_window_size(&self, window: u64, width: u32, height: u32) {
        self.shared
            .windows
            .lock()
            .insert(window, (width, height, Instant::now()));
    }

    pub fn remove_window(&self, window: u64) {
        self.shared.windows.lock().remove(&window);
    }
}

impl Drop for Player {
    fn drop(&mut self) {
        self.shared.stopped.store(true, Ordering::Release);
    }
}

fn youtube_id(input: &str) -> Result<String> {
    let url = url::Url::parse(input)?;
    ensure!(
        url.scheme() == "https" && url.username().is_empty() && url.password().is_none(),
        "Use an HTTPS YouTube video URL"
    );
    let id = match url.host_str() {
        Some("youtu.be") => url
            .path_segments()
            .and_then(|mut segments| segments.next())
            .map(str::to_owned),
        Some("youtube.com" | "www.youtube.com" | "m.youtube.com") => {
            if url.path() == "/watch" {
                url.query_pairs()
                    .find(|(key, _)| key == "v")
                    .map(|(_, value)| value.into_owned())
            } else {
                let mut segments = url.path_segments().into_iter().flatten();
                match segments.next() {
                    Some("live" | "shorts" | "embed") => segments.next().map(str::to_owned),
                    _ => None,
                }
            }
        }
        _ => None,
    };
    let id = id.ok_or_else(|| {
        anyhow::anyhow!("Expected a YouTube watch, live, shorts, or youtu.be URL")
    })?;
    ensure!(
        id.len() == 11
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte)),
        "Invalid YouTube video ID"
    );
    Ok(id)
}

fn desired_height(windows: &[(u32, u32)], source_width: u32, source_height: u32, fit: Fit) -> u32 {
    windows
        .iter()
        .map(|&(width, height)| {
            let horizontal =
                f64::from(width) * f64::from(source_height) / f64::from(source_width.max(1));
            match fit {
                Fit::Cover => horizontal.max(f64::from(height)),
                Fit::Contain => horizontal.min(f64::from(height)),
            }
            .ceil()
            .clamp(2.0, 8192.0) as u32
        })
        .max()
        .unwrap_or(720)
}

fn loop_position(elapsed: Duration, duration: f64) -> f64 {
    if duration.is_finite() && duration > 0.0 {
        elapsed.as_secs_f64() % duration
    } else {
        elapsed.as_secs_f64()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_sources_and_limits() {
        assert!(youtube_id("https://youtu.be/abcdefghijk?t=5").is_ok());
        assert!(youtube_id("https://youtube.com.evil.test/watch?v=abcdefghijk").is_err());
        assert!(youtube_id("https://www.youtube.com/playlist?list=abcdefghijk").is_err());
        assert!(
            Settings {
                max_fps: 0,
                ..Settings::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            Settings {
                opacity: f32::NAN,
                ..Settings::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn resolution_accounts_for_crop_and_multiple_windows() {
        assert_eq!(
            desired_height(&[(3840, 2160)], 1920, 1080, Fit::Cover),
            2160
        );
        assert_eq!(desired_height(&[(2000, 500)], 1920, 1080, Fit::Cover), 1125);
        assert_eq!(
            desired_height(&[(2000, 500)], 1920, 1080, Fit::Contain),
            500
        );
        assert_eq!(
            desired_height(&[(640, 480), (1920, 1080)], 1920, 1080, Fit::Cover),
            1080
        );
    }

    #[test]
    fn stalls_skip_time_instead_of_slowing_playback() {
        assert_eq!(loop_position(Duration::from_secs(125), 60.0), 5.0);
        assert_eq!(loop_position(Duration::from_secs(125), 0.0), 125.0);
    }

    #[test]
    fn static_image_loads_off_thread_without_external_decoders() -> Result<()> {
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("background.png");
            image::RgbaImage::from_pixel(8, 8, image::Rgba([11, 22, 33, 255])).save(&path)?;
            let player = Player::new(
                Settings {
                    source: Some(Source::Image { path }),
                    ffmpeg_path: "missing-ffmpeg".into(),
                    ..Settings::default()
                },
                directory.path().join("cache"),
                Arc::new(http_client::BlockedHttpClient::new()),
            )?;
            player.set_window_size(1, 8, 8);
            let deadline = Instant::now() + Duration::from_secs(3);
            loop {
                let snapshot = player.snapshot();
                if let Some(error) = snapshot.error {
                    anyhow::bail!("{error}");
                }
                if let Some(frame) = snapshot.frame {
                    assert_eq!(frame.format, PixelFormat::Bgra);
                    assert_eq!(frame.data.get(..4), Some([33, 22, 11, 255].as_slice()));
                    return Ok(());
                }
                ensure!(Instant::now() < deadline, "Image never loaded");
                playback::wall_timer(Duration::from_millis(10)).await;
            }
        })
    }
}
