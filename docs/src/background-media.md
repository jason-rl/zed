# Background Media

You can display an image, animated GIF, video, or YouTube livestream behind your
editor and panels. Background media is disabled by default. Audio is never played.

## Configure a background {#configuration}

Use {#action zed::OpenSettingsFile} from the command palette. This feature is
configured in your **user** settings JSON, not project settings or the Settings
Editor. Changes apply without restarting Zed.

```json [settings]
{
  "background_media": {
    "source": {
      "type": "image",
      "path": "~/Pictures/background.png"
    },
    "opacity": 0.2,
    "fit": "cover"
  }
}
```

Paths must be absolute or start with `~/`. On Windows, escape backslashes in JSON,
for example `"C:\\Users\\You\\Pictures\\background.png"`.

| Source type     | Required source field | Playback                                           |
| --------------- | --------------------- | -------------------------------------------------- |
| `image`         | `path`                | Static image                                       |
| `gif`           | `path`                | Repeats indefinitely                               |
| `video`         | `path`                | Repeats indefinitely, without audio                |
| `youtube_video` | `url`                 | Repeats from a persistent local cache              |
| `youtube_live`  | `url`                 | Follows the live stream                            |
| `holodex`       | `channel_ids`         | Follows a live stream from an ordered channel list |

Set `"source": null` to disable playback and restore normal theme backgrounds.
The current-line highlight, scrollbars, terminal cell backgrounds, agent prompt,
and tab hover backgrounds become translucent. Text, icons, cursors, selections,
buttons, context menus, tooltips, and dialogs retain their theme colors.

## Dependencies {#dependencies}

Static images need no external programs. GIFs and all videos require **FFmpeg and
ffprobe** on Zed's `PATH`. Use a current FFmpeg release supporting
`readrate_initial_burst` and `readrate_catchup`.

Every YouTube source, including Holodex, additionally requires **yt-dlp** and any
JavaScript runtime required by your yt-dlp installation. Zed does not install or
update these programs. See the [FFmpeg downloads](https://ffmpeg.org/download.html)
and [yt-dlp installation instructions](https://github.com/yt-dlp/yt-dlp#installation).

You can set `ffmpeg_path`, `ffprobe_path`, and `yt_dlp_path` to absolute executable
paths if they are not on Zed's `PATH`. These are executable paths, not shell commands.
Dependency or playback errors appear as notifications.

## YouTube and Holodex {#youtube}

```json [settings]
{
  "background_media": {
    "source": {
      "type": "youtube_video",
      "url": "https://www.youtube.com/watch?v=VIDEO_ID_HERE"
    },
    "cache_size_mb": 10240
  }
}
```

Replace the placeholder with an actual 11-character video ID. Use `youtube_live`
instead for a currently live URL. Playlists and non-YouTube URLs are not accepted.

For Holodex, get an API key from the
[Holodex API documentation](https://docs.holodex.net/), then run
{#action background_media::SetHolodexApiKey} from the command palette. Paste the
key into the masked field and save it. Zed stores it in the operating system's
credential store: normally Login Keychain on macOS. Development builds also use
the native credential store for this key, never the development credentials file.
macOS may ask you to allow access, especially after rebuilding Zed.

The stored key takes priority over `HOLODEX_API_KEY`. If no key is stored, you can
still supply that environment variable when starting Zed. Use
{#action background_media::RemoveHolodexApiKey} to remove the stored key. The
environment-variable fallback remains active if set. Saving or removing a key
reloads Holodex playback in open windows without restarting Zed. Changes made
directly in Keychain Access require restarting Zed.

The key is not written to settings.json or passed to FFmpeg or yt-dlp. Configure
the channel IDs in your preferred order:

```json [settings]
{
  "background_media": {
    "source": {
      "type": "holodex",
      "channel_ids": ["UCxxxxxxxxxxxxxxxxxxxxxx", "UCyyyyyyyyyyyyyyyyyyyyyy"]
    }
  }
}
```

Zed polls approximately once per minute, choosing the first channel that is live.
It keeps the selected broadcast until Holodex confirms it has ended. A newly live,
higher-priority channel does not interrupt it. If a channel has several live
broadcasts, the most recently started one is preferred.

Finite videos download while the first pass plays. Once a rendition is completely
cached, replays use the local file without YouTube requests. If the download has
not completed by the first replay, playback can wait for completion. Completed
cache entries survive restarts; livestreams are not persistently cached.

The cache lives in Zed's platform cache folder under `background-media`. The
`cache_size_mb` limit includes partial downloads. Unused entries are evicted first;
playing and preparing renditions are protected. If the limit cannot accommodate a
download, increase it or choose a lower `max_height`.

## Quality and resource use {#quality}

The default settings are:

```json [settings]
{
  "background_media": {
    "source": null,
    "opacity": 0.2,
    "fit": "cover",
    "max_fps": 30,
    "max_height": null,
    "hardware_acceleration": "auto",
    "cache_size_mb": 10240,
    "ffmpeg_path": "ffmpeg",
    "ffprobe_path": "ffprobe",
    "yt_dlp_path": "yt-dlp"
  }
}
```

`cover` crops to fill the window; `contain` preserves the whole image. `opacity`
ranges from 0 to 1. `max_fps` ranges from 1 to 60. `max_height` is an optional
resolution ceiling in physical pixels; `null` adapts to the largest visible Zed
window, including display scaling and cropping.

On resize, Zed prepares a suitable YouTube rendition while the current one keeps
playing. Finite-video replacements are fully cached before switching. A live
replacement must align to the current source timeline; if it cannot, the current
stream remains in use. Upgrades wait briefly for resizing to settle, and downgrades
wait longer to avoid frequent switching.

Playback follows elapsed time, not focus events or UI frame counts. Under load,
frames can be skipped without slowing the video's clock. Media subprocesses and
image/cache work run at low priority with bounded buffers. Hardware decoding is
attempted automatically and software decoding is used when it fails. Set
`hardware_acceleration` to `"off"` to disable hardware decoding. GPU color
conversion remains enabled for video rendering.

Hardware decoding and supported codecs depend on your FFmpeg build and GPU. High
resolutions still require memory bandwidth and GPU uploads; lower `max_height` or
`max_fps` if needed. See [Appearance](./appearance.md) for other visual settings.
