use crate::{
    Frame, HardwareAcceleration, PixelFormat, Settings, Shared, Source,
    cache::{Cache, Entry, Record, quality_key},
    desired_height, loop_position, youtube_id,
};
use anyhow::{Context as _, Result, bail, ensure};
use futures::{AsyncReadExt as _, FutureExt as _};
use http_client::{AsyncBody, HttpClient, HttpRequestExt as _, Request};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

// Two pipelines share a 128 MiB queue budget; a third cannot start until presentation acknowledges the handoff.
const FRAME_MEMORY: usize = 64 * 1024 * 1024;
static FRAME_SEQUENCE: AtomicU64 = AtomicU64::new(1);
static PIPELINE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct MediaInfo {
    pub width: u32,
    pub height: u32,
    pub duration: f64,
}

pub(crate) struct Process {
    child: Option<util::process::Child>,
    _stderr: smol::Task<()>,
    diagnostics: Arc<Mutex<VecDeque<String>>>,
    timestamps: smol::channel::Receiver<f64>,
}

impl Process {
    pub fn spawn(mut command: Command) -> Result<Self> {
        command.env_remove("HOLODEX_API_KEY");
        low_priority_command(&mut command);
        let executable = command.get_program().to_string_lossy().into_owned();
        let mut child = util::process::Child::spawn(
            command,
            Stdio::piped(),
            Stdio::piped(),
            Stdio::piped(),
        )
        .map_err(|error| {
            anyhow::anyhow!(
                "Cannot start {executable}: {error}; install it or set its background_media executable path"
            )
        })?;
        let stderr = child.stderr.take().context("Missing subprocess stderr")?;
        let (sender, timestamps) = smol::channel::bounded(32);
        let diagnostics = Arc::new(Mutex::new(VecDeque::new()));
        let task = smol::spawn({
            let diagnostics = diagnostics.clone();
            async move {
                let mut reader = stderr;
                let mut line = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    match reader.read(&mut buffer).await {
                        Ok(0) => {
                            if !line.is_empty() {
                                retain_diagnostic(&diagnostics, &String::from_utf8_lossy(&line));
                            }
                            break;
                        }
                        Ok(count) => {
                            for byte in &buffer[..count] {
                                if *byte == b'\n' {
                                    let text = String::from_utf8_lossy(&line);
                                    if text.contains("Parsed_showinfo") {
                                        if let Some(value) = text
                                            .split("pts_time:")
                                            .nth(1)
                                            .and_then(|value| value.split_whitespace().next())
                                            .and_then(|value| value.parse::<f64>().ok())
                                        {
                                            if sender.send(value).await.is_err() {
                                                return;
                                            }
                                        }
                                    } else {
                                        retain_diagnostic(&diagnostics, &text);
                                    }
                                    line.clear();
                                } else if line.len() < 16384 {
                                    line.push(*byte);
                                }
                            }
                        }
                        Err(error) => {
                            log::debug!("Media subprocess diagnostics closed: {error}");
                            break;
                        }
                    }
                }
            }
        });
        Ok(Self {
            child: Some(child),
            _stderr: task,
            diagnostics,
            timestamps,
        })
    }

    pub fn stdout(&mut self) -> Result<smol::process::ChildStdout> {
        self.child
            .as_mut()
            .and_then(|child| child.stdout.take())
            .context("Missing media subprocess output")
    }

    fn stdin(&mut self) -> Result<smol::process::ChildStdin> {
        self.child
            .as_mut()
            .and_then(|child| child.stdin.take())
            .context("Missing media subprocess input")
    }

    pub async fn status(&mut self) -> Result<ExitStatus> {
        self.child
            .as_mut()
            .context("Media process already closed")?
            .status()
            .await
            .map_err(Into::into)
    }

    async fn diagnostics(&mut self) -> String {
        // Process exit and stderr EOF can be observed in either order.
        (&mut self._stderr).await;
        self.diagnostics
            .lock()
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub(crate) async fn check_status(&mut self, context: &str) -> Result<()> {
        let status = self.status().await?;
        if !status.success() {
            let diagnostics = self.diagnostics().await;
            bail!("{context} ({status})\n{diagnostics}");
        }
        Ok(())
    }
}

fn retain_diagnostic(diagnostics: &Mutex<VecDeque<String>>, line: &str) {
    let line = line.trim();
    if line.is_empty() {
        return;
    }
    let lowercase = line.to_ascii_lowercase();
    let redacted = if ["authorization:", "cookie:", "x-apikey:", "bearer "]
        .iter()
        .any(|header| lowercase.contains(header))
    {
        "[redacted sensitive diagnostic]".to_owned()
    } else {
        line.split_whitespace()
            .map(|word| {
                if word.contains("://") || word.starts_with("//") {
                    "[redacted URL]"
                } else {
                    word
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .filter(|character| !character.is_control())
            .take(512)
            .collect()
    };
    let mut diagnostics = diagnostics.lock();
    if diagnostics.len() == 8 {
        diagnostics.pop_front();
    }
    diagnostics.push_back(redacted);
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if let Err(error) = child.kill() {
                log::debug!("Media process cleanup: {error}");
            }
            smol::spawn(async move {
                if let Err(error) = child.status().await {
                    log::debug!("Media process reap: {error}");
                }
            })
            .detach();
        }
    }
}

pub(crate) async fn output(command: Command) -> Result<Vec<u8>> {
    let executable = command.get_program().to_string_lossy().into_owned();
    let mut process = Process::spawn(command)?;
    let mut reader = process.stdout()?.take(8 * 1024 * 1024 + 1);
    let operation = async {
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await?;
        ensure!(
            bytes.len() <= 8 * 1024 * 1024,
            "Media metadata exceeds the size limit"
        );
        let status = process.status().await?;
        if !status.success() {
            let diagnostics = process.diagnostics().await;
            bail!("Media metadata request failed: {executable} ({status})\n{diagnostics}");
        }
        Ok(bytes)
    };
    futures::pin_mut!(operation);
    futures::select_biased! {
        result = operation.fuse() => result,
        _ = wall_timer(Duration::from_secs(45)).fuse() => bail!("Media metadata request timed out"),
    }
}

pub(crate) fn youtube_command(settings: &Settings) -> Command {
    let mut command = Command::new(&settings.yt_dlp_path);
    command.args([
        "--ignore-config",
        "--no-plugin-dirs",
        "--no-playlist",
        "--no-warnings",
        "--no-update",
        "--no-mark-watched",
        "--socket-timeout",
        "15",
        "--retries",
        "2",
    ]);
    command
}

pub(crate) async fn probe(settings: &Settings, path: &Path) -> Result<MediaInfo> {
    let mut command = Command::new(&settings.ffprobe_path);
    command
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,duration:format=duration",
            "-of",
            "json",
        ])
        .arg(path);
    let value: serde_json::Value = serde_json::from_slice(&output(command).await?)?;
    let stream = value
        .get("streams")
        .and_then(|streams| streams.get(0))
        .context("No video stream found")?;
    let width = stream
        .get("width")
        .and_then(|value| value.as_u64())
        .context("Missing video width")? as u32;
    let height = stream
        .get("height")
        .and_then(|value| value.as_u64())
        .context("Missing video height")? as u32;
    let duration = value
        .pointer("/format/duration")
        .or_else(|| stream.get("duration"))
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse().ok())
        .unwrap_or(0.0);
    ensure!(width > 0 && height > 0, "Invalid video dimensions");
    Ok(MediaInfo {
        width,
        height,
        duration,
    })
}

#[derive(Clone)]
struct Prepared {
    info: MediaInfo,
    highest_available: u32,
    selected_for_height: u32,
    input: String,
    headers: Vec<(String, String)>,
    live: bool,
    cache: Option<Arc<Entry>>,
    alpha: bool,
}

#[derive(Deserialize)]
struct Youtube {
    id: String,
    #[serde(default)]
    duration: Option<f64>,
    #[serde(default)]
    is_live: bool,
    #[serde(default)]
    live_status: Option<String>,
    #[serde(default)]
    formats: Vec<YoutubeFormat>,
    #[serde(default)]
    http_headers: std::collections::BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct YoutubeFormat {
    format_id: String,
    url: Option<String>,
    width: Option<u32>,
    height: Option<u32>,
    fps: Option<f64>,
    vcodec: Option<String>,
    acodec: Option<String>,
    dynamic_range: Option<String>,
    #[serde(default)]
    has_drm: bool,
    #[serde(default)]
    http_headers: std::collections::BTreeMap<String, String>,
}

async fn prepare_youtube(
    settings: &Settings,
    cache: &Cache,
    id: &str,
    target: u32,
    live: bool,
    complete: bool,
) -> Result<Prepared> {
    if !live {
        if let Some(entry) = cache.lookup(id, target).await? {
            return Ok(cached(entry, target));
        }
    }
    let mut command = youtube_command(settings);
    command
        .args([
            "--dump-single-json",
            "--skip-download",
            "--no-live-from-start",
            "--",
        ])
        .arg(format!("https://www.youtube.com/watch?v={id}"));
    let metadata = match output(command).await {
        Ok(metadata) => metadata,
        Err(error) => {
            if !live {
                if let Some(entry) = cache.lookup(id, 0).await? {
                    return Ok(cached(entry, target));
                }
            }
            return Err(error);
        }
    };
    let youtube: Youtube = serde_json::from_slice(&metadata)?;
    ensure!(youtube.id == id, "YouTube resolved to an unexpected video");
    if live {
        ensure!(
            youtube.is_live,
            "Livestream is not currently live ({})",
            youtube.live_status.as_deref().unwrap_or("offline")
        );
    } else {
        ensure!(
            !youtube.is_live
                && youtube
                    .duration
                    .is_some_and(|duration| duration > 0.0 && duration.is_finite()),
            "Use youtube_live for livestreams"
        );
    }
    let mut formats = youtube
        .formats
        .iter()
        .filter(|format| {
            !format.has_drm
                && format.url.is_some()
                && format.height.is_some_and(|height| height > 0)
                && format.width.is_some_and(|width| width > 0)
                && format
                    .height
                    .is_some_and(|height| height <= settings.max_height.unwrap_or(8192))
                && format
                    .vcodec
                    .as_deref()
                    .is_some_and(|codec| codec != "none")
                && format
                    .dynamic_range
                    .as_deref()
                    .is_none_or(|range| range == "SDR")
        })
        .collect::<Vec<_>>();
    let highest = formats
        .iter()
        .filter_map(|format| format.height)
        .max()
        .context("No playable SDR video rendition")?;
    formats.sort_by_key(|format| {
        (
            quality_key(format.height.unwrap_or(0), target),
            format.acodec.as_deref() != Some("none"),
            format
                .fps
                .is_some_and(|fps| fps > f64::from(settings.max_fps)),
            !format
                .vcodec
                .as_deref()
                .is_some_and(|codec| codec.starts_with("avc1")),
        )
    });
    let format = formats.first().context("No video format available")?;
    let info = MediaInfo {
        width: format.width.context("Missing width")?,
        height: format.height.context("Missing height")?,
        duration: youtube.duration.unwrap_or(0.0),
    };
    if !live {
        let record = Record {
            video_id: id.into(),
            format_id: format.format_id.clone(),
            info,
            highest_available: highest,
            bytes: 0,
        };
        let entry = cache.download(record, settings).await?;
        if complete {
            while !entry.complete.load(Ordering::Acquire) {
                if let Some(error) = entry.error.lock().clone() {
                    bail!("{error}");
                }
                wall_timer(Duration::from_millis(100)).await;
            }
        }
        Ok(cached(entry, target))
    } else {
        let mut headers = youtube.http_headers.clone();
        headers.extend(format.http_headers.clone());
        ensure!(
            headers
                .iter()
                .all(|(name, value)| !name.contains(['\r', '\n', ':'])
                    && !value.contains(['\r', '\n'])),
            "Invalid stream headers"
        );
        Ok(Prepared {
            info,
            highest_available: highest,
            selected_for_height: target,
            input: format.url.clone().context("Missing stream URL")?,
            live: true,
            headers: headers.into_iter().collect(),
            cache: None,
            alpha: false,
        })
    }
}

fn cached(entry: Arc<Entry>, target: u32) -> Prepared {
    Prepared {
        info: entry.record.info.clone(),
        highest_available: entry.record.highest_available,
        selected_for_height: target,
        input: entry.path.to_string_lossy().into_owned(),
        headers: Vec::new(),
        live: false,
        cache: Some(entry),
        alpha: false,
    }
}

async fn prepare(
    settings: &Settings,
    cache: &Cache,
    source: &Source,
    target: u32,
    complete: bool,
) -> Result<Prepared> {
    match source {
        Source::Gif { path } | Source::Video { path } => {
            let path = expand_path(path)?;
            let info = probe(settings, &path).await?;
            ensure!(
                info.duration.is_finite() && info.duration > 0.0,
                "Cannot loop media without a finite duration"
            );
            Ok(Prepared {
                highest_available: info.height,
                selected_for_height: target,
                info,
                input: path.to_string_lossy().into_owned(),
                headers: Vec::new(),
                live: false,
                cache: None,
                alpha: matches!(source, Source::Gif { .. }),
            })
        }
        Source::YoutubeVideo { url } => {
            prepare_youtube(settings, cache, &youtube_id(url)?, target, false, complete).await
        }
        Source::YoutubeLive { url } => {
            prepare_youtube(settings, cache, &youtube_id(url)?, target, true, complete).await
        }
        _ => bail!("No media source selected"),
    }
}

struct Decoder {
    progressive: bool,
    prepared: Prepared,
    frames: Arc<Mutex<VecDeque<Arc<Frame>>>>,
    done: Arc<AtomicBool>,
    error: Arc<Mutex<Option<String>>>,
    _task: smol::Task<()>,
    _feed: Option<smol::Task<()>>,
    fps: u32,
    height: u32,
}

impl Decoder {
    fn needs_quality(&self, target: u32) -> bool {
        self.height != target.min(self.prepared.info.height).max(2) / 2 * 2
            // A rendition's dimensions are not the source's maximum quality. Remember the
            // last selection request so an offline fallback does not repeatedly hit YouTube.
            || (target > self.prepared.selected_for_height
                && self.prepared.info.height < target.min(self.prepared.highest_available))
    }

    fn start(
        settings: &Settings,
        prepared: Prepared,
        target: u32,
        position: f64,
        software: bool,
    ) -> Result<Self> {
        let height = target.min(prepared.info.height).max(2) / 2 * 2;
        let width = ((f64::from(height) * f64::from(prepared.info.width)
            / f64::from(prepared.info.height))
        .round() as u32)
            .max(2)
            / 2
            * 2;
        ensure!(
            width <= 16384 && height <= 8192,
            "Video exceeds renderer dimensions"
        );
        let pixels = (width as usize)
            .checked_mul(height as usize)
            .context("Video dimensions overflow")?;
        let format = if prepared.alpha {
            PixelFormat::Bgra
        } else {
            PixelFormat::Nv12
        };
        let bytes = match format {
            PixelFormat::Bgra => pixels.checked_mul(4),
            PixelFormat::Nv12 => pixels.checked_mul(3).map(|bytes| bytes / 2),
        }
        .context("Video frame too large")?;
        ensure!(
            bytes <= FRAME_MEMORY / 2,
            "Video frame exceeds the playback memory budget; lower max_height"
        );
        let capacity = (FRAME_MEMORY / bytes).clamp(2, 18);
        let fps = settings.max_fps.min(((capacity - 1) * 4) as u32).max(1);
        let progressive = prepared
            .cache
            .as_ref()
            .is_some_and(|entry| !entry.complete.load(Ordering::Acquire));
        let mut command = Command::new(&settings.ffmpeg_path);
        command.args([
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "info",
            "-threads",
            "2",
            "-filter_threads",
            "1",
        ]);
        if !software
            && settings.hardware_acceleration == HardwareAcceleration::Auto
            && !prepared.alpha
        {
            command.args(["-hwaccel", "auto"]);
        }
        if !prepared.live {
            // Decoder startup and B-frame reordering need more lead than the handoff lookahead.
            // The bounded frame queue still applies backpressure to this initial burst.
            command.args([
                "-readrate",
                "1",
                "-readrate_initial_burst",
                "2",
                "-readrate_catchup",
                "2",
            ]);
            if !progressive {
                let seek = loop_position(
                    Duration::from_secs_f64(position.max(0.0)),
                    prepared.info.duration,
                );
                command.args(["-stream_loop", "-1", "-ss", &format!("{seek:.6}")]);
            }
        } else {
            command.args(["-rw_timeout", "15000000", "-copyts"]);
        }
        if !prepared.headers.is_empty() {
            command.arg("-headers").arg(
                prepared
                    .headers
                    .iter()
                    .map(|(name, value)| format!("{name}: {value}\r\n"))
                    .collect::<String>(),
            );
        }
        command.arg("-i").arg(if progressive {
            "pipe:0"
        } else {
            &prepared.input
        });
        let pixel_format = if format == PixelFormat::Nv12 {
            "nv12"
        } else {
            "bgra"
        };
        let time_filter = if prepared.live {
            String::new()
        } else {
            "setpts=PTS-STARTPTS,".into()
        };
        // Normalize the CPU/GPU boundary to full-range BT.709; renderers need no codec-specific guesses.
        command.args(["-map", "0:v:0", "-an", "-sn", "-dn", "-vf"])
            .arg(format!("{time_filter}fps={fps},scale={width}:{height}:out_color_matrix=bt709:out_range=full,format={pixel_format},showinfo=checksum=0"))
            .args(["-fps_mode", "passthrough", "-f", "rawvideo", "pipe:1"]);
        let mut process = Process::spawn(command)?;
        let mut reader = process.stdout()?;
        let feed = if progressive {
            let entry = prepared
                .cache
                .clone()
                .context("Missing progressive cache")?;
            let input = process.stdin()?;
            Some(smol::spawn(async move {
                if let Err(error) = entry.copy_to(input).await {
                    log::debug!("Background cache reader stopped: {error}");
                }
            }))
        } else {
            None
        };
        let frames = Arc::new(Mutex::new(VecDeque::new()));
        let pipeline = PIPELINE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let done = Arc::new(AtomicBool::new(false));
        let error = Arc::new(Mutex::new(None));
        let task = smol::spawn({
            let frames = frames.clone();
            let done = done.clone();
            let error = error.clone();
            let live = prepared.live;
            async move {
                let result = async {
                    loop {
                        while frames.lock().len() >= capacity {
                            wall_timer(Duration::from_millis(5)).await;
                        }
                        let mut data = vec![0; bytes];
                        match reader.read_exact(&mut data).await {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                                break;
                            }
                            Err(error) => return Err(error.into()),
                        }
                        let timestamp = process
                            .timestamps
                            .recv()
                            .await
                            .context("Decoder produced a frame without its timestamp")?;
                        ensure!(timestamp.is_finite(), "Invalid video frame timestamp");
                        let frame = Arc::new(Frame {
                            pipeline,
                            width,
                            height,
                            format,
                            data: data.into(),
                            timestamp: if live {
                                timestamp
                            } else {
                                timestamp + if progressive { 0.0 } else { position }
                            },
                            sequence: FRAME_SEQUENCE.fetch_add(1, Ordering::Relaxed),
                        });
                        frames.lock().push_back(frame);
                    }
                    process.check_status("Video decoder failed").await?;
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                if let Err(failure) = result {
                    *error.lock() = Some(failure.to_string());
                }
                done.store(true, Ordering::Release);
            }
        });
        Ok(Self {
            progressive,
            prepared,
            frames,
            done,
            error,
            _task: task,
            _feed: feed,
            fps,
            height,
        })
    }

    fn frame_at(&self, time: f64) -> Option<Arc<Frame>> {
        let mut frames = self.frames.lock();
        let mut frame = None;
        while frames
            .front()
            .is_some_and(|frame| frame.timestamp <= time + 0.5 / f64::from(self.fps))
        {
            frame = frames.pop_front();
        }
        frame
    }

    fn ready(&self, time: f64) -> bool {
        let mut frames = self.frames.lock();
        while frames.get(1).is_some_and(|frame| frame.timestamp <= time) {
            frames.pop_front();
        }
        let tolerance = 1.5 / f64::from(self.fps);
        frames.front().is_some_and(|frame| {
            frame.timestamp >= time - tolerance
                && frame.timestamp <= time + 0.5 / f64::from(self.fps)
        }) && frames
            .back()
            .is_some_and(|frame| frame.timestamp >= time + 0.25 - tolerance)
    }

    fn handoff_frame(&self, time: f64, previous_timestamp: Option<f64>) -> Option<Arc<Frame>> {
        if !self.ready(time) {
            return None;
        }
        // Independently sought decoders can have different frame phases within the clock tolerance.
        self.frame_at(time)
            .filter(|frame| previous_timestamp.is_none_or(|previous| frame.timestamp >= previous))
    }
}

#[derive(Deserialize)]
struct Live {
    id: String,
    status: String,
    start_actual: Option<String>,
    channel: Channel,
}

#[derive(Deserialize)]
struct Channel {
    id: String,
}

async fn holodex(
    http: &Arc<dyn HttpClient>,
    channels: &[String],
    current: Option<&str>,
    key: &str,
) -> Result<Option<String>> {
    let mut streams = Vec::new();
    for channel in channels {
        let mut url = url::Url::parse("https://holodex.net/api/v2/live")?;
        url.query_pairs_mut()
            .append_pair("channel_id", channel)
            .append_pair("status", "live")
            .append_pair("type", "stream")
            .append_pair("include", "live_info")
            .append_pair("limit", "50");
        let request = Request::get(url.as_str())
            .header("X-APIKEY", key)
            .timeout(Duration::from_secs(15))
            .body(AsyncBody::empty())?;
        let response = http.send(request).await?;
        ensure!(
            response.status().is_success(),
            "Holodex returned {}; check the API key or retry later",
            response.status()
        );
        let mut bytes = Vec::new();
        response
            .into_body()
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .await?;
        ensure!(bytes.len() <= 1024 * 1024, "Holodex response is too large");
        streams.extend(serde_json::from_slice::<Vec<Live>>(&bytes)?);
    }
    if let Some(current) = current {
        if !streams
            .iter()
            .any(|stream| stream.id == current && stream.status == "live")
        {
            let request = Request::get(format!("https://holodex.net/api/v2/videos/{current}"))
                .header("X-APIKEY", key)
                .timeout(Duration::from_secs(15))
                .body(AsyncBody::empty())?;
            let response = http.send(request).await?;
            ensure!(
                response.status().is_success(),
                "Could not confirm whether the current Holodex stream ended"
            );
            let mut bytes = Vec::new();
            response
                .into_body()
                .take(1024 * 1024 + 1)
                .read_to_end(&mut bytes)
                .await?;
            ensure!(bytes.len() <= 1024 * 1024, "Holodex response is too large");
            let stream: Live = serde_json::from_slice(&bytes)?;
            if stream.status != "past" {
                return Ok(Some(current.to_owned()));
            }
        }
    }
    Ok(select_live(&streams, channels, current))
}

fn select_live(streams: &[Live], channels: &[String], current: Option<&str>) -> Option<String> {
    if let Some(current) = current {
        if streams
            .iter()
            .any(|stream| stream.id == current && stream.status == "live")
        {
            return Some(current.into());
        }
    }
    for channel in channels {
        if let Some(stream) = streams
            .iter()
            .filter(|stream| &stream.channel.id == channel && stream.status == "live")
            .max_by(|left, right| {
                left.start_actual
                    .cmp(&right.start_actual)
                    .then_with(|| left.id.cmp(&right.id))
            })
        {
            return Some(stream.id.clone());
        }
    }
    None
}

pub(crate) async fn run(
    settings: Settings,
    directory: PathBuf,
    http: Arc<dyn HttpClient>,
    shared: &Arc<Shared>,
    holodex_key: Option<String>,
) -> Result<()> {
    let Some(configured_source) = settings.source.clone() else {
        return Ok(());
    };
    if let Source::Image { path } = configured_source {
        return static_image(&settings, path, shared).await;
    }
    let cache = Cache::new(directory, settings.cache_size_mb * 1024 * 1024);
    let mut source = configured_source.clone();
    let mut active: Option<Decoder> = None;
    let mut candidate: Option<Decoder> = None;
    let mut retired: Option<(Decoder, u64)> = None;
    let mut candidate_since = Instant::now();
    let mut source_changed = false;
    let mut preparing: Option<smol::Task<Result<Prepared>>> = None;
    let mut live_query: Option<smol::Task<Result<Option<String>>>> = None;
    let mut current_live: Option<String> = None;
    let mut last_live_query = Instant::now()
        .checked_sub(Duration::from_secs(60))
        .unwrap_or_else(Instant::now);
    let mut anchor = Instant::now();
    let mut media_origin = 0.0;
    let mut requested_height = 720;
    let mut preparing_height = 720;
    let mut requested_since = Instant::now();
    let mut retry_after = Instant::now();
    let mut software = false;
    let mut pressure_height = u32::MAX;
    let mut healthy_since = Instant::now();
    let mut last_delivery = Instant::now();
    let mut first_frame = true;
    while !shared.stopped.load(Ordering::Acquire) {
        let now = Instant::now();
        if retired
            .as_ref()
            .is_some_and(|(_, pipeline)| shared.presented.load(Ordering::Acquire) == *pipeline)
        {
            retired = None;
        }
        let windows = shared
            .windows
            .lock()
            .values()
            .filter(|(_, _, seen)| now.duration_since(*seen) < Duration::from_secs(2))
            .map(|&(width, height, _)| (width, height))
            .collect::<Vec<_>>();
        if let Source::Holodex { channel_ids } = &configured_source {
            if live_query.is_none()
                && now.duration_since(last_live_query) >= Duration::from_secs(60)
            {
                last_live_query = now;
                live_query = Some(smol::spawn({
                    let http = http.clone();
                    let channels = channel_ids.clone();
                    let current = current_live.clone();
                    let key = holodex_key.clone();
                    async move {
                        let key = key.as_deref().filter(|key| !key.trim().is_empty())
                            .context("Use Background Media: Set Holodex API Key or set HOLODEX_API_KEY to use Holodex")?;
                        holodex(&http, &channels, current.as_deref(), key).await
                    }
                }));
            }
            if let Some(query) = &mut live_query {
                if let Some(result) = query.now_or_never() {
                    live_query = None;
                    match result {
                        Ok(next) if next != current_live => {
                            current_live = next;
                            preparing = None;
                            candidate = None;
                            first_frame = true;
                            source_changed = true;
                            if let Some(id) = &current_live {
                                source = Source::YoutubeLive {
                                    url: format!("https://www.youtube.com/watch?v={id}"),
                                };
                            } else {
                                source = configured_source.clone();
                                active = None;
                                retired = None;
                                shared.snapshot.lock().frame = None;
                            }
                        }
                        Ok(_) => {}
                        Err(error) => shared.snapshot.lock().error = Some(error.to_string()),
                    }
                }
            }
        }
        if matches!(source, Source::Holodex { .. }) {
            wall_timer(Duration::from_millis(50)).await;
            continue;
        }
        let (width, height) = active
            .as_ref()
            .map(|decoder| (decoder.prepared.info.width, decoder.prepared.info.height))
            .unwrap_or((16, 9));
        let target = desired_height(&windows, width, height, settings.fit)
            .min(settings.max_height.unwrap_or(8192))
            .min(pressure_height);
        if target != requested_height {
            requested_height = target;
            requested_since = now;
            preparing = None;
            candidate = None;
        }
        let time = media_origin + now.duration_since(anchor).as_secs_f64();
        if let Some(decoder) = &active {
            if let Some(frame) = decoder.frame_at(time) {
                if time - frame.timestamp > 0.5 {
                    pressure_height = (decoder.height / 2).max(144).min(decoder.height);
                    healthy_since = now;
                    if pressure_height == decoder.height {
                        source_changed = true;
                    }
                } else {
                    last_delivery = now;
                    if !windows.is_empty() {
                        shared.snapshot.lock().frame = Some(frame);
                    }
                }
            }
            let exhausted =
                decoder.done.load(Ordering::Acquire) && decoder.frames.lock().is_empty();
            let cached_replay = decoder.progressive
                && decoder
                    .prepared
                    .cache
                    .as_ref()
                    .is_some_and(|entry| entry.complete.load(Ordering::Acquire));
            if exhausted || cached_replay {
                if let Some(error) = decoder.error.lock().clone() {
                    shared.snapshot.lock().error = Some(error);
                    software = true;
                }
                // Progressive input becomes seekable only when its full payload is committed.
                if decoder
                    .prepared
                    .cache
                    .as_ref()
                    .is_none_or(|entry| entry.complete.load(Ordering::Acquire))
                {
                    if candidate.is_none()
                        && preparing.is_none()
                        && retired.is_none()
                        && now >= retry_after
                    {
                        if decoder.prepared.live {
                            source_changed = true;
                        } else {
                            match Decoder::start(
                                &settings,
                                decoder.prepared.clone(),
                                target,
                                time + 0.5,
                                software,
                            ) {
                                Ok(replacement) => {
                                    candidate = Some(replacement);
                                    candidate_since = now;
                                }
                                Err(error) => {
                                    shared.snapshot.lock().error = Some(error.to_string());
                                    retry_after = now + Duration::from_secs(10);
                                }
                            }
                        }
                    }
                }
            }
            if now.duration_since(last_delivery) > Duration::from_secs(2) {
                pressure_height = (decoder.height / 2).max(144).min(decoder.height);
                healthy_since = now;
            } else if now.duration_since(healthy_since) > Duration::from_secs(10) {
                pressure_height = u32::MAX;
            }
        }
        if let Some(error) = candidate
            .as_ref()
            .and_then(|decoder| decoder.prepared.cache.as_ref())
            .and_then(|entry| entry.error.lock().clone())
        {
            shared.snapshot.lock().error = Some(error);
            candidate = None;
            retry_after = now + Duration::from_secs(10);
        }
        let failed_cache = active
            .as_ref()
            .and_then(|decoder| decoder.prepared.cache.as_ref())
            .and_then(|entry| entry.error.lock().clone());
        if let Some(error) = failed_cache {
            shared.snapshot.lock().error = Some(error);
            active = None;
            candidate = None;
            preparing = None;
            first_frame = true;
            retry_after = now + Duration::from_secs(10);
        }
        let delay = active
            .as_ref()
            .map(|decoder| {
                if target < decoder.height && pressure_height != u32::MAX {
                    Duration::ZERO
                } else if target > decoder.height {
                    Duration::from_millis(500)
                } else {
                    Duration::from_secs(5)
                }
            })
            .unwrap_or(Duration::ZERO);
        let needs_quality = active
            .as_ref()
            .is_none_or(|decoder| decoder.needs_quality(target));
        if preparing.is_none()
            && candidate.is_none()
            && retired.is_none()
            && now >= retry_after
            && (needs_quality || source_changed)
            && now.duration_since(requested_since) >= delay
        {
            preparing_height = target;
            preparing = Some(smol::spawn({
                let settings = settings.clone();
                let cache = cache.clone();
                let source = source.clone();
                let complete = active.is_some();
                async move { prepare(&settings, &cache, &source, target, complete).await }
            }));
        }
        if let Some(task) = &mut preparing {
            if let Some(result) = task.now_or_never() {
                preparing = None;
                match result {
                    Ok(prepared) => {
                        let position = if active.is_some() && !prepared.live {
                            media_origin + now.duration_since(anchor).as_secs_f64() + 0.5
                        } else {
                            0.0
                        };
                        match Decoder::start(
                            &settings,
                            prepared,
                            preparing_height,
                            position,
                            software,
                        ) {
                            Ok(decoder) => {
                                if active.is_none() {
                                    anchor = now;
                                    media_origin = 0.0;
                                }
                                candidate = Some(decoder);
                                candidate_since = now;
                            }
                            Err(error) => {
                                shared.snapshot.lock().error = Some(error.to_string());
                                retry_after = now + Duration::from_secs(10);
                            }
                        }
                    }
                    Err(error) => {
                        shared.snapshot.lock().error = Some(error.to_string());
                        retry_after = now + Duration::from_secs(10);
                    }
                }
            }
        }
        if let Some(decoder) = &candidate {
            let first = decoder.frames.lock().front().cloned();
            let switch_time = if first_frame {
                first.as_ref().map(|frame| frame.timestamp)
            } else {
                Some(media_origin + now.duration_since(anchor).as_secs_f64())
            };
            if let Some(time) = switch_time {
                let previous_timestamp = if first_frame {
                    None
                } else {
                    shared
                        .snapshot
                        .lock()
                        .frame
                        .as_ref()
                        .map(|frame| frame.timestamp)
                };
                if let Some(frame) = decoder.handoff_frame(time, previous_timestamp) {
                    let pipeline = frame.pipeline;
                    shared.snapshot.lock().frame = Some(frame);
                    shared.snapshot.lock().error = None;
                    media_origin = time;
                    anchor = now;
                    first_frame = false;
                    last_delivery = now;
                    if let Some(previous) = active.take() {
                        retired = Some((previous, pipeline));
                    }
                    active = candidate.take();
                    source_changed = false;
                }
            }
            if candidate.as_ref().is_some_and(|decoder| {
                (decoder.done.load(Ordering::Acquire) && decoder.frames.lock().is_empty())
                    || now.duration_since(candidate_since) > Duration::from_secs(20)
            }) {
                if let Some(error) = candidate
                    .as_ref()
                    .and_then(|decoder| decoder.error.lock().clone())
                {
                    shared.snapshot.lock().error = Some(error);
                }
                candidate = None;
                software = true;
                retry_after = now + Duration::from_secs(10);
                shared.snapshot.lock().error.get_or_insert_with(|| "Replacement stream could not align with the playback clock; keeping the current stream".into());
            }
        }
        wall_timer(Duration::from_millis(10)).await;
    }
    Ok(())
}

fn expand_path(path: &Path) -> Result<PathBuf> {
    if let Ok(suffix) = path.strip_prefix("~") {
        let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
            .context("Cannot resolve home directory")?;
        Ok(PathBuf::from(home).join(suffix))
    } else {
        ensure!(
            path.is_absolute(),
            "Background media paths must be absolute or start with ~/"
        );
        Ok(path.to_owned())
    }
}

async fn static_image(settings: &Settings, path: PathBuf, shared: &Arc<Shared>) -> Result<()> {
    let path = expand_path(&path)?;
    let image = background_work(move || -> Result<_> {
        let mut reader = image::ImageReader::open(path)?.with_guessed_format()?;
        let mut limits = image::Limits::default();
        limits.max_alloc = Some(256 * 1024 * 1024);
        reader.limits(limits);
        Ok(reader.decode()?.into_rgba8())
    })
    .await?;
    let image = Arc::new(image);
    let mut last_size = (0, 0);
    while !shared.stopped.load(Ordering::Acquire) {
        let windows = shared
            .windows
            .lock()
            .values()
            .map(|&(width, height, _)| (width, height))
            .collect::<Vec<_>>();
        let height = desired_height(&windows, image.width(), image.height(), settings.fit)
            .min(image.height())
            .min(settings.max_height.unwrap_or(8192));
        let width = ((f64::from(height) * f64::from(image.width()) / f64::from(image.height()))
            as u32)
            .max(1);
        if (width, height) != last_size {
            last_size = (width, height);
            let image = image.clone();
            let frame = background_work(move || -> Result<_> {
                ensure!(
                    u64::from(width) * u64::from(height) * 4 <= FRAME_MEMORY as u64,
                    "Background image exceeds the frame memory budget; lower max_height"
                );
                let mut destination = fast_image_resize::images::Image::new(
                    width,
                    height,
                    fast_image_resize::PixelType::U8x4,
                );
                fast_image_resize::Resizer::new().resize(
                    image.as_ref(),
                    &mut destination,
                    &fast_image_resize::ResizeOptions::new(),
                )?;
                let mut bytes = destination.into_vec();
                for pixel in bytes.chunks_exact_mut(4) {
                    pixel.swap(0, 2);
                }
                Ok(Frame {
                    pipeline: 0,
                    width,
                    height,
                    format: PixelFormat::Bgra,
                    data: bytes.into(),
                    timestamp: 0.0,
                    sequence: FRAME_SEQUENCE.fetch_add(1, Ordering::Relaxed),
                })
            })
            .await?;
            shared.snapshot.lock().frame = Some(Arc::new(frame));
        }
        wall_timer(Duration::from_millis(500)).await;
    }
    Ok(())
}

#[allow(
    clippy::disallowed_methods,
    reason = "Media timing must run independently of GPUI focus and its dispatcher; GPUI tests do not drive this engine"
)]
pub(crate) fn wall_timer(duration: Duration) -> smol::Timer {
    smol::Timer::after(duration)
}

pub(crate) async fn background_work<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    type Job = Box<dyn FnOnce() + Send>;
    static WORKER: std::sync::OnceLock<std::result::Result<std::sync::mpsc::Sender<Job>, String>> =
        std::sync::OnceLock::new();
    let worker = WORKER
        .get_or_init(|| {
            let (sender, receiver) = std::sync::mpsc::channel::<Job>();
            std::thread::Builder::new()
                .name("background-media-io".into())
                .spawn(move || {
                    lower_thread_priority();
                    for operation in receiver {
                        operation();
                    }
                })
                .map(|_| sender)
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(|error| anyhow::anyhow!("Cannot start media worker: {error}"))?;
    let (sender, receiver) = futures::channel::oneshot::channel();
    worker
        .send(Box::new(move || {
            if !sender.is_canceled() && sender.send(operation()).is_err() {
                log::debug!("Background media work was cancelled");
            }
        }))
        .map_err(|_| anyhow::anyhow!("Background media worker stopped"))?;
    receiver
        .await
        .context("Background media operation cancelled")?
}

pub(crate) fn lower_thread_priority() {
    #[cfg(target_os = "macos")]
    unsafe {
        if libc::setpriority(libc::PRIO_DARWIN_THREAD, 0, libc::PRIO_DARWIN_BG) != 0 {
            log::debug!(
                "Could not lower media thread priority: {}",
                std::io::Error::last_os_error()
            );
        }
    }
    #[cfg(target_os = "linux")]
    unsafe {
        if libc::setpriority(libc::PRIO_PROCESS, 0, 19) != 0 {
            log::debug!(
                "Could not lower media thread priority: {}",
                std::io::Error::last_os_error()
            );
        }
    }
    #[cfg(windows)]
    unsafe {
        use windows::Win32::System::Threading::{
            GetCurrentThread, SetThreadPriority, THREAD_MODE_BACKGROUND_BEGIN,
        };
        if let Err(error) = SetThreadPriority(GetCurrentThread(), THREAD_MODE_BACKGROUND_BEGIN) {
            log::debug!("Could not lower media thread priority: {error}");
        }
    }
}

fn low_priority_command(command: &mut Command) {
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt as _;
        unsafe {
            command.pre_exec(|| {
                if libc::setpriority(libc::PRIO_PROCESS, 0, 19) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                #[cfg(target_os = "linux")]
                if libc::syscall(libc::SYS_ioprio_set, 1, 0, 3 << 13) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt as _;
        command.creation_flags(0x08000000 | 0x00000040);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires network access, yt-dlp, ffmpeg, and ffprobe"]
    fn network_youtube_video_decodes() -> Result<()> {
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            let settings = Settings {
                source: Some(Source::YoutubeVideo {
                    url: "https://www.youtube.com/watch?v=bNjiriUjFSo".into(),
                }),
                ..Settings::default()
            };
            let player = crate::Player::new_with_holodex_key(
                settings,
                directory.path().to_owned(),
                Arc::new(http_client::BlockedHttpClient::new()),
                None,
            )?;
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(110) {
                player.set_window_size(1, 1920, 1080);
                let snapshot = player.snapshot();
                if let Some(error) = snapshot.error {
                    bail!("{error}");
                }
                if let Some(frame) = snapshot.frame {
                    player.presented(frame.pipeline);
                    if frame.timestamp > 73.0 {
                        return Ok(());
                    }
                }
                wall_timer(Duration::from_millis(10)).await;
            }
            bail!("YouTube playback did not complete two loops")
        })
    }

    #[test]
    fn youtube_command_disables_updates_with_a_supported_option() {
        let command = youtube_command(&Settings::default());
        assert!(command.get_args().any(|argument| argument == "--no-update"));
        assert!(
            !command
                .get_args()
                .any(|argument| argument == "--no-check-updates")
        );
    }

    #[cfg(unix)]
    #[test]
    fn media_subprocesses_do_not_inherit_holodex_api_keys() -> Result<()> {
        smol::block_on(async {
            let mut command = Command::new("/bin/sh");
            command.env("HOLODEX_API_KEY", "test-key");
            command.args(["-c", "printf '%s' \"${HOLODEX_API_KEY+present}\""]);
            ensure!(
                output(command).await?.is_empty(),
                "Holodex credential inherited by subprocess"
            );
            Ok(())
        })
    }

    #[cfg(unix)]
    #[test]
    fn decoder_and_download_failures_report_redacted_diagnostics() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            let executable = directory.path().join("failing-media-tool");
            std::fs::write(
                &executable,
                "#!/bin/sh\nprintf '%s' 'ERROR: test failure https://example.invalid/?token=secret' >&2\nexit 1\n",
            )?;
            std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700))?;
            let settings = Settings {
                ffmpeg_path: executable.clone(),
                yt_dlp_path: executable,
                ..Settings::default()
            };
            let prepared = buffered_decoder(&[]).prepared.clone();
            let decoder = Decoder::start(&settings, prepared.clone(), 2, 0.0, true)?;
            let cache = Cache::new(directory.path().join("cache"), 1024 * 1024);
            let entry = cache
                .download(
                    Record {
                        video_id: "abcdefghijk".into(),
                        format_id: "test".into(),
                        info: prepared.info,
                        highest_available: 2,
                        bytes: 0,
                    },
                    &settings,
                )
                .await?;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let decoder_error = decoder.error.lock().clone();
                let download_error = entry.error.lock().clone();
                if let (Some(decoder_error), Some(download_error)) = (decoder_error, download_error)
                {
                    assert!(decoder_error.contains("Video decoder failed"));
                    assert!(download_error.contains("YouTube download failed"));
                    for error in [decoder_error, download_error] {
                        assert!(error.contains("ERROR: test failure [redacted URL]"));
                        assert!(!error.contains("secret"));
                    }
                    return Ok(());
                }
                ensure!(
                    Instant::now() < deadline,
                    "Process failures were not reported"
                );
                wall_timer(Duration::from_millis(10)).await;
            }
        })
    }

    #[test]
    fn subprocess_diagnostics_are_bounded_and_redacted() {
        let diagnostics = Mutex::new(VecDeque::new());
        for _ in 0..20 {
            retain_diagnostic(&diagnostics, &"é".repeat(2000));
        }
        assert_eq!(diagnostics.lock().len(), 8);
        assert!(
            diagnostics
                .lock()
                .iter()
                .all(|line| line.chars().count() == 512)
        );
        retain_diagnostic(
            &diagnostics,
            "ERROR: failed opening 'https://example.invalid/video?sig=secret'",
        );
        retain_diagnostic(&diagnostics, "Authorization: Bearer secret");
        retain_diagnostic(&diagnostics, "Cookie: session=secret");
        let lines = diagnostics.lock();
        assert!(
            lines
                .iter()
                .any(|line| line == "ERROR: failed opening [redacted URL]")
        );
        assert!(lines.iter().all(|line| !line.contains("secret")));
    }

    #[cfg(unix)]
    #[test]
    fn metadata_failure_reports_stderr_including_the_unterminated_last_line() -> Result<()> {
        smol::block_on(async {
            let mut command = Command::new("/bin/sh");
            command.args([
                "-c",
                "printf '%s\\n' 'ERROR: https://example.invalid/?sig=secret' >&2; printf '%s' 'yt-dlp: error: no such option: --bad-option' >&2; exit 2",
            ]);
            let error = output(command)
                .await
                .expect_err("Subprocess should fail")
                .to_string();
            assert!(error.contains("Media metadata request failed: /bin/sh"));
            assert!(error.contains("yt-dlp: error: no such option: --bad-option"));
            assert!(error.contains("[redacted URL]"));
            assert!(!error.contains("secret"));
            Ok(())
        })
    }

    #[test]
    #[ignore = "requires yt-dlp on PATH; validates real argument parsing without network access"]
    fn installed_youtube_extractor_accepts_command_options() -> Result<()> {
        smol::block_on(async {
            // --version exits before option validation, so deliberately omit the URL instead.
            let error = output(youtube_command(&Settings::default()))
                .await
                .expect_err("yt-dlp requires a URL")
                .to_string();
            ensure!(
                error.contains("You must provide at least one URL"),
                "{error}"
            );
            ensure!(!error.contains("no such option"), "{error}");
            Ok(())
        })
    }

    fn buffered_decoder(timestamps: &[f64]) -> Decoder {
        Decoder {
            progressive: false,
            prepared: Prepared {
                highest_available: 2,
                selected_for_height: 2,
                info: MediaInfo {
                    width: 2,
                    height: 2,
                    duration: 2.0,
                },
                input: String::new(),
                headers: Vec::new(),
                live: false,
                cache: None,
                alpha: false,
            },
            frames: Arc::new(Mutex::new(
                timestamps
                    .iter()
                    .map(|timestamp| {
                        Arc::new(Frame {
                            pipeline: 1,
                            width: 2,
                            height: 2,
                            format: PixelFormat::Nv12,
                            data: Arc::from([0; 6]),
                            timestamp: *timestamp,
                            sequence: 1,
                        })
                    })
                    .collect(),
            )),
            done: Arc::new(AtomicBool::new(false)),
            error: Arc::new(Mutex::new(None)),
            _task: smol::spawn(std::future::pending()),
            _feed: None,
            fps: 30,
            height: 2,
        }
    }

    #[test]
    fn late_render_drops_frames_without_slowing_clock() {
        let decoder = buffered_decoder(&[0.0, 0.1, 0.2, 0.3, 0.4]);
        assert_eq!(
            decoder.frame_at(0.31).map(|frame| frame.timestamp),
            Some(0.3)
        );
        assert_eq!(decoder.frames.lock().len(), 1);
        assert!(decoder.frame_at(0.32).is_none());
    }

    #[test]
    fn resizing_can_upgrade_beyond_the_current_youtube_rendition() {
        let mut decoder = buffered_decoder(&[]);
        decoder.height = 720;
        decoder.prepared.info.height = 720;
        decoder.prepared.highest_available = 2160;
        decoder.prepared.selected_for_height = 720;
        assert!(!decoder.needs_quality(720));
        assert!(decoder.needs_quality(1080));
        assert!(decoder.needs_quality(480));

        decoder.prepared.selected_for_height = 1080;
        assert!(!decoder.needs_quality(1080));
        assert!(decoder.needs_quality(2160));

        decoder.prepared.highest_available = 720;
        assert!(!decoder.needs_quality(2160));
    }

    #[test]
    fn handoff_requires_alignment_and_lookahead_across_loop_boundaries() {
        let decoder = buffered_decoder(&[4.9, 5.0, 5.1, 5.2, 5.3]);
        assert!(decoder.ready(5.0));
        assert!(!decoder.ready(3.0));
        assert!(!decoder.ready(6.0));
        let short = buffered_decoder(&[5.0, 5.03]);
        assert!(!short.ready(5.0));
    }

    #[test]
    fn handoff_waits_for_a_frame_that_does_not_rewind_the_previous_pipeline() {
        let decoder = buffered_decoder(&[5.0, 5.033, 5.066, 5.1, 5.3]);
        assert!(decoder.handoff_frame(5.0, Some(5.01)).is_none());
        assert_eq!(
            decoder
                .handoff_frame(5.04, Some(5.01))
                .map(|frame| frame.timestamp),
            Some(5.033)
        );
        let new_source = buffered_decoder(&[0.0, 0.1, 0.3]);
        assert!(new_source.handoff_frame(0.0, None).is_some());
    }

    #[test]
    #[ignore = "requires ffmpeg and ffprobe on PATH"]
    fn ffmpeg_loops_and_hands_off_without_rewinding() -> Result<()> {
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("fixture.mp4");
            let mut command = Command::new("ffmpeg");
            command
                .args([
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "testsrc2=size=640x360:rate=30",
                    "-t",
                    "1",
                    "-c:v",
                    "libx264",
                    "-pix_fmt",
                    "yuv420p",
                ])
                .arg(&path);
            output(command).await?;
            let settings = Settings {
                hardware_acceleration: HardwareAcceleration::Off,
                ..Settings::default()
            };
            let cache = Cache::new(directory.path().join("cache"), 1024 * 1024);
            let prepared = prepare(&settings, &cache, &Source::Video { path }, 180, false).await?;
            let decoder = Decoder::start(&settings, prepared.clone(), 180, 0.0, true)?;
            let start = Instant::now();
            let mut anchor = None;
            let mut candidate = None;
            let mut switched = false;
            let mut last_timestamp = 0.0;
            while start.elapsed() < Duration::from_secs(8) {
                if anchor.is_none() && decoder.ready(0.0) {
                    anchor = Some(Instant::now());
                }
                if let Some(anchor) = anchor {
                    let time = anchor.elapsed().as_secs_f64();
                    if let Some(frame) = decoder.frame_at(time) {
                        assert!(frame.timestamp >= last_timestamp);
                        last_timestamp = frame.timestamp;
                    }
                    if time > 1.2 && candidate.is_none() {
                        candidate = Some(Decoder::start(
                            &settings,
                            prepared.clone(),
                            360,
                            time + 0.5,
                            true,
                        )?);
                    }
                    if candidate
                        .as_ref()
                        .is_some_and(|candidate| candidate.ready(time))
                    {
                        let replacement = candidate
                            .as_ref()
                            .and_then(|candidate| candidate.frame_at(time))
                            .context("Missing replacement frame")?;
                        assert!((replacement.timestamp - time).abs() < 0.1);
                        assert_eq!(replacement.height, 360);
                        switched = true;
                        break;
                    }
                }
                if let Some(error) = decoder.error.lock().clone() {
                    bail!("{error}");
                }
                wall_timer(Duration::from_millis(10)).await;
            }
            ensure!(
                last_timestamp > 1.0,
                "Video did not loop: last timestamp {last_timestamp}"
            );
            ensure!(switched, "Resolution handoff never became ready");
            Ok(())
        })
    }

    #[test]
    fn holodex_does_not_preempt_current_stream() {
        let streams = vec![
            Live {
                id: "first".into(),
                status: "live".into(),
                start_actual: None,
                channel: Channel { id: "a".into() },
            },
            Live {
                id: "second".into(),
                status: "live".into(),
                start_actual: None,
                channel: Channel { id: "b".into() },
            },
        ];
        let channels = vec!["a".into(), "b".into()];
        assert_eq!(
            select_live(&streams, &channels, None).as_deref(),
            Some("first")
        );
        assert_eq!(
            select_live(&streams, &channels, Some("second")).as_deref(),
            Some("second")
        );
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires ffmpeg and ffprobe on PATH; replaces only the YouTube network boundary"]
    fn youtube_download_is_cached_and_reopened_without_extractor_requests() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("fixture.mp4");
            let mut command = Command::new("ffmpeg");
            command
                .args([
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "testsrc2=size=320x180:rate=15",
                    "-t",
                    "1",
                    "-c:v",
                    "libx264",
                    "-movflags",
                    "frag_keyframe+empty_moov",
                    "-pix_fmt",
                    "yuv420p",
                ])
                .arg(&path);
            output(command).await?;
            let requests = directory.path().join("requests");
            let extractor = directory.path().join("yt-dlp");
            let quote = |value: &str| format!("'{}'", value.replace('\'', "'\"'\"'"));
            let metadata = serde_json::json!({
                "id": "abcdefghijk", "duration": 1.0, "formats": [{"format_id": "test", "url": "https://example.invalid/video", "width": 320, "height": 180, "vcodec": "avc1", "acodec": "none", "fps": 15}]
            });
            std::fs::write(
                &extractor,
                format!(
                    "#!/bin/sh\nprintf x >> {}\ncase \" $* \" in *' --dump-single-json '*) printf '%s\\n' {};; *) cat {};; esac\n",
                    quote(&requests.to_string_lossy()),
                    quote(&metadata.to_string()),
                    quote(&path.to_string_lossy())
                ),
            )?;
            std::fs::set_permissions(&extractor, std::fs::Permissions::from_mode(0o700))?;
            let mut settings = Settings {
                yt_dlp_path: extractor,
                hardware_acceleration: HardwareAcceleration::Off,
                ..Settings::default()
            };
            let cache = Cache::new(directory.path().join("cache"), 1024 * 1024);
            let prepared =
                prepare_youtube(&settings, &cache, "abcdefghijk", 180, false, false).await?;
            let decoder = Decoder::start(&settings, prepared.clone(), 180, 0.0, true)?;
            let deadline = Instant::now() + Duration::from_secs(10);
            let entry = prepared.cache.as_ref().context("Video not cached")?;
            while !entry.complete.load(Ordering::Acquire) {
                ensure!(Instant::now() < deadline, "Cache never completed");
                if let Some(error) = entry.error.lock().clone() {
                    bail!("{error}");
                }
                decoder.frame_at(0.0);
                wall_timer(Duration::from_millis(10)).await;
            }
            drop(decoder);
            drop(prepared);
            settings.yt_dlp_path = directory.path().join("extractor-must-not-be-run");
            let cached =
                prepare_youtube(&settings, &cache, "abcdefghijk", 180, false, true).await?;
            let decoder = Decoder::start(&settings, cached, 180, 0.0, true)?;
            let start = Instant::now();
            let mut latest = 0.0;
            while start.elapsed() < Duration::from_secs(3) {
                if let Some(frame) = decoder.frame_at(start.elapsed().as_secs_f64()) {
                    latest = frame.timestamp;
                }
                wall_timer(Duration::from_millis(10)).await;
            }
            ensure!(latest > 2.0, "Cached rendition failed to replay");
            assert_eq!(std::fs::read(requests)?, b"xx");
            Ok(())
        })
    }

    #[test]
    #[ignore = "requires ffmpeg and ffprobe on PATH"]
    fn animated_gif_loops_with_alpha_frames() -> Result<()> {
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            let path = directory.path().join("fixture.gif");
            let mut command = Command::new("ffmpeg");
            command
                .args([
                    "-hide_banner",
                    "-loglevel",
                    "error",
                    "-f",
                    "lavfi",
                    "-i",
                    "testsrc2=size=32x32:rate=10",
                    "-t",
                    "0.5",
                ])
                .arg(&path);
            output(command).await?;
            let settings = Settings::default();
            let cache = Cache::new(directory.path().join("cache"), 1024 * 1024);
            let prepared = prepare(&settings, &cache, &Source::Gif { path }, 32, false).await?;
            let decoder = Decoder::start(&settings, prepared, 32, 0.0, false)?;
            let start = Instant::now();
            let mut latest = 0.0;
            while start.elapsed() < Duration::from_secs(2) {
                if let Some(frame) = decoder.frame_at(start.elapsed().as_secs_f64()) {
                    latest = frame.timestamp;
                    assert_eq!(frame.format, PixelFormat::Bgra);
                }
                if let Some(error) = decoder.error.lock().clone() {
                    bail!("{error}");
                }
                wall_timer(Duration::from_millis(10)).await;
            }
            ensure!(latest > 1.0, "GIF did not loop");
            Ok(())
        })
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires ffmpeg and ffprobe on PATH; replaces only the YouTube network boundary"]
    fn youtube_resize_keeps_playing_until_the_higher_rendition_is_ready() -> Result<()> {
        use std::os::unix::fs::PermissionsExt as _;
        smol::block_on(async {
            let directory = tempfile::tempdir()?;
            for (name, size) in [("low", "320x180"), ("high", "640x360")] {
                let mut command = Command::new("ffmpeg");
                command
                    .args(["-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i"])
                    .arg(format!("testsrc2=size={size}:rate=30"))
                    .args([
                        "-t",
                        "2",
                        "-c:v",
                        "libx264",
                        "-movflags",
                        "frag_keyframe+empty_moov",
                        "-pix_fmt",
                        "yuv420p",
                    ])
                    .arg(directory.path().join(format!("{name}.mp4")));
                output(command).await?;
            }
            let extractor = directory.path().join("yt-dlp");
            let quote = |value: &str| format!("'{}'", value.replace('\'', "'\"'\"'"));
            let metadata = serde_json::json!({
                "id": "abcdefghijk", "duration": 2.0, "formats": [
                    {"format_id": "low", "url": "https://example.invalid/low", "width": 320, "height": 180, "vcodec": "avc1", "acodec": "none", "fps": 30},
                    {"format_id": "high", "url": "https://example.invalid/high", "width": 640, "height": 360, "vcodec": "avc1", "acodec": "none", "fps": 30}
                ]
            });
            std::fs::write(
                &extractor,
                format!(
                    "#!/bin/sh\ncase \" $* \" in *' --dump-single-json '*) printf '%s\\n' {};; *' --format high '*) sleep 1; cat {};; *) cat {};; esac\n",
                    quote(&metadata.to_string()),
                    quote(&directory.path().join("high.mp4").to_string_lossy()),
                    quote(&directory.path().join("low.mp4").to_string_lossy()),
                ),
            )?;
            std::fs::set_permissions(&extractor, std::fs::Permissions::from_mode(0o700))?;
            let shared = Arc::new(Shared {
                presented: AtomicU64::new(0),
                snapshot: Mutex::new(Default::default()),
                windows: Mutex::new(std::collections::BTreeMap::from([(
                    1,
                    (320, 180, Instant::now()),
                )])),
                stopped: AtomicBool::new(false),
            });
            let settings = Settings {
                source: Some(Source::YoutubeVideo {
                    url: "https://youtu.be/abcdefghijk".into(),
                }),
                yt_dlp_path: extractor,
                hardware_acceleration: HardwareAcceleration::Off,
                ..Settings::default()
            };
            let task = smol::spawn({
                let shared = shared.clone();
                let directory = directory.path().join("cache");
                async move {
                    run(
                        settings,
                        directory,
                        Arc::new(http_client::BlockedHttpClient::new()),
                        &shared,
                        None,
                    )
                    .await
                }
            });
            let deadline = Instant::now() + Duration::from_secs(15);
            let mut resized_at = None;
            let mut last_sequence = 0;
            let mut last_timestamp = 0.0;
            let mut low_frames_during_resize = 0;
            let mut switched = false;
            while Instant::now() < deadline {
                let height = if resized_at.is_some() { 360 } else { 180 };
                shared
                    .windows
                    .lock()
                    .insert(1, (height * 16 / 9, height, Instant::now()));
                let snapshot = shared.snapshot.lock().clone();
                if let Some(error) = snapshot.error {
                    bail!("{error}");
                }
                if let Some(frame) = snapshot
                    .frame
                    .filter(|frame| frame.sequence != last_sequence)
                {
                    shared.presented.store(frame.pipeline, Ordering::Release);
                    assert!(
                        frame.timestamp >= last_timestamp,
                        "Playback moved backwards from {last_timestamp} to {} on pipeline {} at height {}",
                        frame.timestamp,
                        frame.pipeline,
                        frame.height,
                    );
                    last_sequence = frame.sequence;
                    last_timestamp = frame.timestamp;
                    if frame.height == 180 {
                        if resized_at.is_some() {
                            low_frames_during_resize += 1;
                        } else {
                            resized_at = Some(Instant::now());
                        }
                    } else if frame.height == 360 && resized_at.is_some() {
                        switched = true;
                        break;
                    }
                }
                wall_timer(Duration::from_millis(10)).await;
            }
            shared.stopped.store(true, Ordering::Release);
            task.await?;
            ensure!(switched, "YouTube did not upgrade after resize");
            ensure!(
                low_frames_during_resize >= 10,
                "Old rendition stopped during buffering"
            );
            Ok(())
        })
    }
}
