use crate::background_media_credentials::{self, HolodexKey};
use crate::{MessageNotification, Workspace, WorkspaceSettings, notifications::NotificationId};
use futures::future::Shared;
use gpui::{
    App, Context, Entity, Global, IntoElement, ObjectFit, Render, RenderImage, Styled, Task,
    Window, WindowId, div, img, surface,
};
use media_background::{Fit, PixelFormat, Player, Settings};
use settings::Settings as _;
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use theme::ActiveTheme;
use ui::prelude::*;
use util::ResultExt as _;

#[derive(Default)]
struct SharedPlayer(Option<(Settings, u64, Weak<Player>)>);
impl Global for SharedPlayer {}

pub struct BackgroundMediaEnvironment(pub Shared<Task<()>>);
impl Global for BackgroundMediaEnvironment {}

pub(crate) struct BackgroundMedia {
    player: Option<Arc<Player>>,
    settings: Settings,
    credentials_revision: u64,
    window_id: WindowId,
    sequence: u64,
    pipeline: u64,
    image: Option<Arc<RenderImage>>,
    surface: Option<Arc<gpui::Nv12Frame>>,
    theme: Option<Arc<theme::Theme>>,
    error: Option<String>,
    _task: Task<()>,
}

impl BackgroundMedia {
    pub fn new(
        workspace: gpui::WeakEntity<Workspace>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) -> Self {
        if !cx.has_global::<SharedPlayer>() {
            cx.set_global(SharedPlayer::default());
        }
        cx.on_release(|this, cx| theme::set_media_background(this.window_id, false, cx))
            .detach();
        let environment = cx
            .try_global::<BackgroundMediaEnvironment>()
            .map(|environment| environment.0.clone());
        let task = cx.spawn_in(window, async move |this, cx| {
            // GUI launches inherit a minimal PATH until the login shell environment has loaded.
            if let Some(environment) = environment {
                environment.await;
            }
            loop {
                let delay = match this.update_in(cx, |this, window, cx| {
                    let settings = WorkspaceSettings::get_global(cx).background_media.clone();
                    let uses_holodex = matches!(
                        settings.source,
                        Some(media_background::Source::Holodex { .. })
                    );
                    if uses_holodex {
                        background_media_credentials::ensure_loaded(cx);
                    }
                    let credentials_revision = if uses_holodex {
                        cx.global::<HolodexKey>().revision
                    } else {
                        0
                    };
                    let credentials_ready =
                        !uses_holodex || cx.global::<HolodexKey>().value.is_some();
                    if this.settings.opacity != settings.opacity {
                        this.settings.opacity = settings.opacity;
                        cx.notify();
                    }
                    if this.settings != settings
                        || this.credentials_revision != credentials_revision
                    {
                        this.settings = settings.clone();
                        this.credentials_revision = credentials_revision;
                        if let Some(player) = this.player.take() {
                            player.remove_window(this.window_id.as_u64());
                        }
                        this.sequence = 0;
                        this.surface = None;
                        if let Some(image) = this.image.take() {
                            window.drop_image(image).log_err();
                        }
                        this.error = None;
                        if settings.source.is_some() && credentials_ready {
                            this.player = cx
                                .global::<SharedPlayer>()
                                .0
                                .as_ref()
                                .filter(|(previous, revision, _)| {
                                    *previous == settings && *revision == credentials_revision
                                })
                                .and_then(|(_, _, player)| player.upgrade());
                            if this.player.is_none() {
                                if let Some(workspace) = workspace.upgrade() {
                                    let http = workspace.read(cx).client().http_client();
                                    let key = if uses_holodex {
                                        cx.global::<HolodexKey>().value.clone().unwrap_or(Ok(None))
                                    } else {
                                        Ok(None)
                                    };
                                    let result = key.map_err(anyhow::Error::msg).and_then(|key| {
                                        Player::new_with_holodex_key(
                                            settings.clone(),
                                            paths::temp_dir().join("background-media"),
                                            http,
                                            key,
                                        )
                                    });
                                    match result {
                                        Ok(player) => {
                                            let player = Arc::new(player);
                                            cx.global_mut::<SharedPlayer>().0 = Some((
                                                settings,
                                                credentials_revision,
                                                Arc::downgrade(&player),
                                            ));
                                            this.player = Some(player);
                                        }
                                        Err(error) => {
                                            let message = error.to_string();
                                            this.error = Some(message.clone());
                                            workspace.update(cx, |workspace, cx| {
                                                workspace.show_notification(
                                                    NotificationId::unique::<BackgroundMedia>(),
                                                    cx,
                                                    move |cx| {
                                                        cx.new(|cx| {
                                                            MessageNotification::new(
                                                                message.clone(),
                                                                cx,
                                                            )
                                                        })
                                                    },
                                                );
                                            });
                                        }
                                    }
                                }
                            }
                        }
                        theme::set_media_background(this.window_id, false, cx);
                        window.refresh();
                    }
                    if let Some(player) = &this.player {
                        let size = window.viewport_size();
                        let scale = window.scale_factor();
                        player.set_window_size(
                            this.window_id.as_u64(),
                            (f32::from(size.width) * scale) as u32,
                            (f32::from(size.height) * scale) as u32,
                        );
                        let snapshot = player.snapshot();
                        if snapshot.frame.is_none() && this.sequence != 0 {
                            this.sequence = 0;
                            this.surface = None;
                            if let Some(image) = this.image.take() {
                                window.drop_image(image).log_err();
                            }
                            theme::set_media_background(this.window_id, false, cx);
                            window.refresh();
                        }
                        if snapshot.error != this.error {
                            this.error = snapshot.error.clone();
                            if let Some(error) = snapshot.error {
                                workspace
                                    .update(cx, |workspace, cx| {
                                        workspace.show_notification(
                                            NotificationId::unique::<BackgroundMedia>(),
                                            cx,
                                            move |cx| {
                                                cx.new(|cx| {
                                                    MessageNotification::new(error.clone(), cx)
                                                })
                                            },
                                        );
                                    })
                                    .log_err();
                            }
                        }
                        if let Some(frame) = snapshot
                            .frame
                            .filter(|frame| frame.sequence != this.sequence)
                        {
                            this.sequence = frame.sequence;
                            this.pipeline = frame.pipeline;
                            match frame.format {
                                PixelFormat::Nv12 => {
                                    this.surface = gpui::Nv12Frame::new(
                                        frame.sequence,
                                        frame.width,
                                        frame.height,
                                        frame.data.clone(),
                                    )
                                    .log_err()
                                    .map(Arc::new);
                                }
                                PixelFormat::Bgra => {
                                    if let Some(buffer) = image::RgbaImage::from_raw(
                                        frame.width,
                                        frame.height,
                                        frame.data.to_vec(),
                                    ) {
                                        let image =
                                            Arc::new(RenderImage::new(vec![image::Frame::new(
                                                buffer,
                                            )]));
                                        if let Some(previous) = this.image.replace(image) {
                                            window.on_next_frame(move |window, _| {
                                                window.drop_image(previous).log_err();
                                            });
                                        }
                                    }
                                }
                            }
                            cx.notify();
                        }
                    }
                    let enabled = this.image.is_some() || this.surface.is_some();
                    if enabled
                        && (this
                            .theme
                            .as_ref()
                            .is_none_or(|previous| !Arc::ptr_eq(previous, cx.theme()))
                            || Arc::ptr_eq(cx.window_theme(window), cx.theme()))
                    {
                        this.theme = Some(cx.theme().clone());
                        theme::set_media_background(this.window_id, true, cx);
                        window.refresh();
                    } else if !enabled {
                        this.theme = None;
                    }
                    if this.player.is_some() {
                        Duration::from_secs_f64(1.0 / f64::from(this.settings.max_fps.max(1)))
                    } else {
                        Duration::from_millis(250)
                    }
                }) {
                    Ok(delay) => delay,
                    Err(_) => break,
                };
                cx.background_executor().timer(delay).await;
            }
        });
        Self {
            player: None,
            settings: Settings::default(),
            credentials_revision: 0,
            window_id: window.window_handle().window_id(),
            sequence: 0,
            pipeline: 0,
            image: None,
            surface: None,
            theme: None,
            error: None,
            _task: task,
        }
    }
}

impl Drop for BackgroundMedia {
    fn drop(&mut self) {
        if let Some(player) = &self.player {
            player.remove_window(self.window_id.as_u64());
        }
    }
}

impl Render for BackgroundMedia {
    fn render(&mut self, window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let fit = || match self.settings.fit {
            Fit::Cover => ObjectFit::Cover,
            Fit::Contain => ObjectFit::Contain,
        };
        if let Some(player) = &self.player {
            let player = player.clone();
            let pipeline = self.pipeline;
            window.on_next_frame(move |_, _| player.presented(pipeline));
        }
        div()
            .absolute()
            .inset_0()
            .size_full()
            .overflow_hidden()
            .bg(cx.theme().colors().background)
            .child(
                div()
                    .size_full()
                    .opacity(self.settings.opacity)
                    .when_some(self.surface.clone(), |element, frame| {
                        element.child(surface(frame).size_full().object_fit(fit()))
                    })
                    .when_some(self.image.clone(), |element, image| {
                        element.child(img(image).size_full().object_fit(fit()))
                    }),
            )
    }
}

pub(crate) fn new(
    workspace: gpui::WeakEntity<Workspace>,
    window: &mut Window,
    cx: &mut App,
) -> Entity<BackgroundMedia> {
    cx.new(|cx| BackgroundMedia::new(workspace, window, cx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{FutureExt as _, channel::oneshot};
    use settings::SettingsStore;

    #[gpui::test(iterations = 10)]
    fn playback_waits_for_shell_environment(cx: &mut gpui::TestAppContext) {
        cx.update(|cx| {
            let mut settings = SettingsStore::test(cx);
            settings
                .set_user_settings(r#"{"background_media":{"opacity":0.15}}"#, cx)
                .expect("Valid media settings");
            cx.set_global(settings);
            theme::init(theme::LoadThemes::JustBase, cx);
        });
        let (sender, receiver) = oneshot::channel();
        let environment = cx.background_executor.spawn(async move {
            receiver.await.expect("Environment initialized");
        });
        cx.set_global(BackgroundMediaEnvironment(environment.shared()));
        let window = cx.add_window(|window, cx| {
            BackgroundMedia::new(gpui::WeakEntity::new_invalid(), window, cx)
        });
        cx.run_until_parked();
        window
            .read_with(cx, |media, _| assert_eq!(media.settings.opacity, 0.2))
            .expect("Read pending media");
        sender.send(()).expect("Release environment gate");
        cx.run_until_parked();
        window
            .read_with(cx, |media, _| assert_eq!(media.settings.opacity, 0.15))
            .expect("Read initialized media");
    }
}
