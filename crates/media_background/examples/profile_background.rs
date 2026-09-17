#[cfg(target_os = "macos")]
fn main() -> anyhow::Result<()> {
    use anyhow::{Context as _, ensure};
    use gpui::{
        Bounds, ContentMask, DevicePixels, Nv12Frame, PaintSurface, PlatformHeadlessRenderer, Quad,
        ScaledPixels, Scene, SurfaceSource, point, size,
    };
    use gpui_apple::metal_renderer::MetalHeadlessRenderer;
    use media_background::{Player, Settings, Source};
    use std::{
        sync::Arc,
        time::{Duration, Instant},
    };

    let path = std::env::args_os()
        .nth(1)
        .context("Pass the absolute path to a 4K video fixture")?;
    let directory = tempfile::tempdir()?;
    let player = Player::new(
        Settings {
            source: Some(Source::Video { path: path.into() }),
            ..Settings::default()
        },
        directory.path().to_owned(),
        Arc::new(http_client::BlockedHttpClient::new()),
    )?;
    let mut renderer = MetalHeadlessRenderer::new();
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut last_sequence = 0;
    for height in [720, 1080, 2160] {
        let width = height * 16 / 9;
        let bounds = Bounds {
            origin: point(ScaledPixels(0.0), ScaledPixels(0.0)),
            size: size(ScaledPixels(width as f32), ScaledPixels(height as f32)),
        };
        let mut samples = Vec::new();
        let mut video_frames = 0;
        let mut reached_height = false;
        let mut validated_pixels = false;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(6) {
            ensure!(Instant::now() < deadline, "Profile exceeded its deadline");
            player.set_window_size(1, width, height);
            let snapshot = player.snapshot();
            if let Some(error) = snapshot.error {
                anyhow::bail!("{error}");
            }
            if let Some(frame) = snapshot.frame {
                let mut scene = Scene::default();
                let frame_source = Arc::new(Nv12Frame::new(
                    frame.sequence,
                    frame.width,
                    frame.height,
                    frame.data.clone(),
                )?);
                scene.insert_primitive(PaintSurface {
                    order: Default::default(),
                    bounds,
                    content_mask: ContentMask { bounds },
                    source: SurfaceSource::Nv12(frame_source),
                    opacity: 0.2,
                });
                for index in 0..100 {
                    scene.insert_primitive(Quad {
                        bounds: Bounds {
                            origin: point(
                                ScaledPixels((index % 10 * 80) as f32),
                                ScaledPixels((index / 10 * 30) as f32),
                            ),
                            size: size(ScaledPixels(60.0), ScaledPixels(15.0)),
                        },
                        content_mask: ContentMask { bounds },
                        background: gpui::rgb(0x888888).into(),
                        ..Quad::default()
                    });
                }
                scene.finish();
                if !validated_pixels {
                    let pixels = renderer.render_scene_to_image(
                        &scene,
                        size(DevicePixels(width as i32), DevicePixels(height as i32)),
                    )?;
                    ensure!(
                        pixels.enumerate_pixels().any(|(x, y, pixel)| x > width / 2
                            && y > height / 2
                            && (pixel[0] != 0 || pixel[1] != 0 || pixel[2] != 0)
                            && pixel[0] <= 53
                            && pixel[1] <= 53
                            && pixel[2] <= 53),
                        "NV12 video or its opacity was not rendered outside the foreground quads"
                    );
                    validated_pixels = true;
                }
                let render_start = Instant::now();
                renderer.render_scene(
                    &scene,
                    size(DevicePixels(width as i32), DevicePixels(height as i32)),
                )?;
                samples.push(render_start.elapsed().as_secs_f64() * 1000.0);
                if frame.sequence != last_sequence {
                    video_frames += 1;
                    last_sequence = frame.sequence;
                }
                reached_height |= frame.height == height;
                player.presented(frame.pipeline);
            }
            std::thread::sleep(Duration::from_millis(8));
        }
        ensure!(reached_height, "Decoder did not reach {height}p");
        samples.sort_by(f64::total_cmp);
        let percentile = samples
            .get(samples.len() * 95 / 100)
            .context("No rendered frames")?;
        println!(
            "{height}p: rendered={}, video_frames={video_frames}, CPU render p95={percentile:.3} ms, max={:.3} ms (includes resize handoff)",
            samples.len(),
            samples.last().context("No samples")?
        );
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn main() {
    eprintln!(
        "This rendering profile requires macOS Metal; run the engine smoke tests on other platforms."
    );
}
