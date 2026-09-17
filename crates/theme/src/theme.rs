#![deny(missing_docs)]

//! # Theme
//!
//! This crate provides the theme system for Zed.
//!
//! ## Overview
//!
//! A theme is a collection of colors used to build a consistent appearance for UI components across the application.

mod buffer_line_height;
mod color_space;
mod default_colors;
mod fallback_themes;
mod font_family_cache;
mod icon_theme;
mod icon_theme_schema;
mod registry;
mod scale;
mod schema;
mod styles;
mod theme_settings_provider;
mod ui_density;

use std::sync::Arc;

use gpui::BorrowAppContext;
use gpui::Global;
use gpui::{
    App, AssetSource, Hsla, Pixels, SharedString, Styled, Tiling, WindowAppearance,
    WindowBackgroundAppearance, px,
};
use serde::Deserialize;

pub use crate::buffer_line_height::*;
pub use crate::color_space::*;
pub use crate::default_colors::*;
pub use crate::fallback_themes::{apply_status_color_defaults, apply_theme_color_defaults};
pub use crate::font_family_cache::*;
pub use crate::icon_theme::*;
pub use crate::icon_theme_schema::*;
pub use crate::registry::*;
pub use crate::scale::*;
pub use crate::schema::*;
pub use crate::styles::*;
pub use crate::theme_settings_provider::*;
pub use crate::ui_density::*;

/// The name of the default dark theme.
pub const DEFAULT_DARK_THEME: &str = "One Dark";

/// Defines window border radius for platforms that use client side decorations.
pub const CLIENT_SIDE_DECORATION_ROUNDING: Pixels = px(10.0);
/// Defines window shadow size for platforms that use client side decorations.
pub const CLIENT_SIDE_DECORATION_SHADOW: Pixels = px(10.0);

/// Styling helpers for elements that follow client-side window decorations.
pub trait ClientDecorationsExt: Styled {
    /// Rounds each corner whose two adjacent edges are both untiled.
    fn rounded_client_corners(mut self, tiling: Tiling) -> Self {
        if !tiling.top && !tiling.left {
            self = self.rounded_tl(CLIENT_SIDE_DECORATION_ROUNDING);
        }
        if !tiling.top && !tiling.right {
            self = self.rounded_tr(CLIENT_SIDE_DECORATION_ROUNDING);
        }
        if !tiling.bottom && !tiling.left {
            self = self.rounded_bl(CLIENT_SIDE_DECORATION_ROUNDING);
        }
        if !tiling.bottom && !tiling.right {
            self = self.rounded_br(CLIENT_SIDE_DECORATION_ROUNDING);
        }
        self
    }
}

impl<T: Styled> ClientDecorationsExt for T {}

/// The appearance of the theme.
#[derive(Debug, PartialEq, Clone, Copy, Deserialize)]
pub enum Appearance {
    /// A light appearance.
    Light,
    /// A dark appearance.
    Dark,
}

impl Appearance {
    /// Returns whether the appearance is light.
    pub fn is_light(&self) -> bool {
        match self {
            Self::Light => true,
            Self::Dark => false,
        }
    }
}

impl From<WindowAppearance> for Appearance {
    fn from(value: WindowAppearance) -> Self {
        match value {
            WindowAppearance::Dark | WindowAppearance::VibrantDark => Self::Dark,
            WindowAppearance::Light | WindowAppearance::VibrantLight => Self::Light,
        }
    }
}

/// Which themes should be loaded. This is used primarily for testing.
pub enum LoadThemes {
    /// Only load the base theme.
    ///
    /// No user themes will be loaded.
    JustBase,

    /// Load all of the built-in themes.
    All(Box<dyn AssetSource>),
}

/// Initialize the theme system with default themes.
///
/// This sets up the [`ThemeRegistry`], [`FontFamilyCache`], [`SystemAppearance`],
/// and [`GlobalTheme`] with the default dark theme. It does NOT load bundled
/// themes from JSON or integrate with settings — use `theme_settings::init` for that.
pub fn init(themes_to_load: LoadThemes, cx: &mut App) {
    SystemAppearance::init(cx);
    let assets = match themes_to_load {
        LoadThemes::JustBase => Box::new(()) as Box<dyn AssetSource>,
        LoadThemes::All(assets) => assets,
    };
    ThemeRegistry::set_global(assets, cx);
    FontFamilyCache::init_global(cx);

    let themes = ThemeRegistry::default_global(cx);
    let theme = themes.get(DEFAULT_DARK_THEME).unwrap_or_else(|_| {
        themes
            .list()
            .into_iter()
            .next()
            .map(|m| themes.get(&m.name).unwrap())
            .unwrap()
    });
    let icon_theme = themes.default_icon_theme().unwrap();
    cx.set_global(GlobalTheme { theme, icon_theme });
}

/// Implementing this trait allows accessing the active theme.
pub trait ActiveTheme {
    /// Returns the active theme.
    fn theme(&self) -> &Arc<Theme>;

    /// Theme surfaces for a workspace displaying background media.
    fn window_theme(&self, window: &gpui::Window) -> &Arc<Theme>;

    /// Tints a foreground surface without fading its text or controls.
    fn media_background_color(&self, window: &gpui::Window, color: Hsla, opacity: f32) -> Hsla {
        if Arc::ptr_eq(self.window_theme(window), self.theme()) {
            color
        } else {
            color.opacity(opacity)
        }
    }
}

impl ActiveTheme for App {
    fn theme(&self) -> &Arc<Theme> {
        GlobalTheme::theme(self)
    }

    fn window_theme(&self, window: &gpui::Window) -> &Arc<Theme> {
        self.try_global::<MediaBackgroundThemes>()
            .and_then(|themes| themes.0.get(&window.window_handle().window_id()))
            .unwrap_or_else(|| self.theme())
    }
}

#[derive(Default)]
struct MediaBackgroundThemes(std::collections::HashMap<gpui::WindowId, Arc<Theme>>);
impl Global for MediaBackgroundThemes {}

/// Enables transparent structural surfaces only in the specified workspace window.
pub fn set_media_background(window: gpui::WindowId, enabled: bool, cx: &mut App) {
    if !cx.has_global::<MediaBackgroundThemes>() {
        cx.set_global(MediaBackgroundThemes::default());
    }
    let theme = if enabled {
        let mut theme = cx.theme().as_ref().clone();
        let colors = &mut theme.styles.colors;
        for color in [
            &mut colors.background,
            &mut colors.editor_background,
            &mut colors.editor_gutter_background,
            &mut colors.terminal_background,
            &mut colors.panel_background,
            &mut colors.toolbar_background,
            &mut colors.status_bar_background,
            &mut colors.title_bar_background,
            &mut colors.title_bar_inactive_background,
            &mut colors.tab_bar_background,
            &mut colors.tab_active_background,
            &mut colors.tab_inactive_background,
        ] {
            color.a = 0.0;
        }
        Some(Arc::new(theme))
    } else {
        None
    };
    let themes = cx.global_mut::<MediaBackgroundThemes>();
    if let Some(theme) = theme {
        themes.0.insert(window, theme);
    } else {
        themes.0.remove(&window);
    }
}

/// The appearance of the system.
#[derive(Debug, Clone, Copy)]
pub struct SystemAppearance(pub Appearance);

impl std::ops::Deref for SystemAppearance {
    type Target = Appearance;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for SystemAppearance {
    fn default() -> Self {
        Self(Appearance::Dark)
    }
}

#[derive(Default)]
struct GlobalSystemAppearance(SystemAppearance);

impl std::ops::DerefMut for GlobalSystemAppearance {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl std::ops::Deref for GlobalSystemAppearance {
    type Target = SystemAppearance;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Global for GlobalSystemAppearance {}

impl SystemAppearance {
    /// Initializes the [`SystemAppearance`] for the application.
    pub fn init(cx: &mut App) {
        *cx.default_global::<GlobalSystemAppearance>() =
            GlobalSystemAppearance(SystemAppearance(cx.window_appearance().into()));
    }

    /// Returns the global [`SystemAppearance`].
    pub fn global(cx: &App) -> Self {
        cx.global::<GlobalSystemAppearance>().0
    }

    /// Returns a mutable reference to the global [`SystemAppearance`].
    pub fn global_mut(cx: &mut App) -> &mut Self {
        cx.global_mut::<GlobalSystemAppearance>()
    }
}

/// A theme family is a grouping of themes under a single name.
///
/// For example, the "One" theme family contains the "One Light" and "One Dark" themes.
///
/// It can also be used to package themes with many variants.
///
/// For example, the "Atelier" theme family contains "Cave", "Dune", "Estuary", "Forest", "Heath", etc.
pub struct ThemeFamily {
    /// The unique identifier for the theme family.
    pub id: String,
    /// The name of the theme family. This will be displayed in the UI, such as when adding or removing a theme family.
    pub name: SharedString,
    /// The author of the theme family.
    pub author: SharedString,
    /// The [Theme]s in the family.
    pub themes: Vec<Theme>,
    /// The color scales used by the themes in the family.
    /// Note: This will be removed in the future.
    pub scales: ColorScales,
}

/// A theme is the primary mechanism for defining the appearance of the UI.
#[derive(Clone, Debug, PartialEq)]
pub struct Theme {
    /// The unique identifier for the theme.
    pub id: String,
    /// The name of the theme.
    pub name: SharedString,
    /// The appearance of the theme (light or dark).
    pub appearance: Appearance,
    /// The colors and other styles for the theme.
    pub styles: ThemeStyles,
}

impl Theme {
    /// Returns the [`SystemColors`] for the theme.
    #[inline(always)]
    pub fn system(&self) -> &SystemColors {
        &self.styles.system
    }

    /// Returns the [`AccentColors`] for the theme.
    #[inline(always)]
    pub fn accents(&self) -> &AccentColors {
        &self.styles.accents
    }

    /// Returns the [`PlayerColors`] for the theme.
    #[inline(always)]
    pub fn players(&self) -> &PlayerColors {
        &self.styles.player
    }

    /// Returns the [`ThemeColors`] for the theme.
    #[inline(always)]
    pub fn colors(&self) -> &ThemeColors {
        &self.styles.colors
    }

    /// Returns the [`SyntaxTheme`] for the theme.
    #[inline(always)]
    pub fn syntax(&self) -> &Arc<SyntaxTheme> {
        &self.styles.syntax
    }

    /// Returns the [`StatusColors`] for the theme.
    #[inline(always)]
    pub fn status(&self) -> &StatusColors {
        &self.styles.status
    }

    /// Returns the [`Appearance`] for the theme.
    #[inline(always)]
    pub fn appearance(&self) -> Appearance {
        self.appearance
    }

    /// Returns the [`WindowBackgroundAppearance`] for the theme.
    #[inline(always)]
    pub fn window_background_appearance(&self) -> WindowBackgroundAppearance {
        self.styles.window_background_appearance
    }

    /// Darkens the color by reducing its lightness.
    /// The resulting lightness is clamped to ensure it doesn't go below 0.0.
    ///
    /// The first value darkens light appearance mode, the second darkens appearance dark mode.
    ///
    /// Note: This is a tentative solution and may be replaced with a more robust color system.
    pub fn darken(&self, color: Hsla, light_amount: f32, dark_amount: f32) -> Hsla {
        let amount = match self.appearance {
            Appearance::Light => light_amount,
            Appearance::Dark => dark_amount,
        };
        let mut hsla = color;
        hsla.l = (hsla.l - amount).max(0.0);
        hsla
    }
}

/// Deserializes an icon theme from the given bytes.
pub fn deserialize_icon_theme(bytes: &[u8]) -> anyhow::Result<IconThemeFamilyContent> {
    let icon_theme_family: IconThemeFamilyContent = serde_json_lenient::from_slice(bytes)?;

    Ok(icon_theme_family)
}

/// The active theme.
pub struct GlobalTheme {
    theme: Arc<Theme>,
    icon_theme: Arc<IconTheme>,
}
impl Global for GlobalTheme {}

impl GlobalTheme {
    /// Creates a new [`GlobalTheme`] with the given theme and icon theme.
    pub fn new(theme: Arc<Theme>, icon_theme: Arc<IconTheme>) -> Self {
        Self { theme, icon_theme }
    }

    /// Updates the active theme.
    pub fn update_theme(cx: &mut App, theme: Arc<Theme>) {
        cx.update_global::<Self, _>(|this, _| this.theme = theme);
    }

    /// Updates the active icon theme.
    pub fn update_icon_theme(cx: &mut App, icon_theme: Arc<IconTheme>) {
        cx.update_global::<Self, _>(|this, _| this.icon_theme = icon_theme);
    }

    /// Returns the active theme.
    pub fn theme(cx: &App) -> &Arc<Theme> {
        &cx.global::<Self>().theme
    }

    /// Returns the active icon theme.
    pub fn icon_theme(cx: &App) -> &Arc<IconTheme> {
        &cx.global::<Self>().icon_theme
    }
}

#[cfg(test)]
mod media_background_tests {
    use super::*;

    #[gpui::test]
    fn media_surfaces_are_window_scoped_and_restore_theme(cx: &mut gpui::TestAppContext) {
        cx.update(|cx| init(LoadThemes::JustBase, cx));
        let media_window = cx.add_window(|_, _| gpui::EmptyView);
        let normal_window = cx.add_window(|_, _| gpui::EmptyView);
        media_window
            .update(cx, |_, window, cx| {
                let original = cx.theme().clone();
                let color = gpui::hsla(0.2, 0.5, 0.6, 0.8);
                assert_eq!(cx.media_background_color(window, color, 0.3), color);
                set_media_background(window.window_handle().window_id(), true, cx);
                let translucent = cx.media_background_color(window, color, 0.3);
                assert_eq!(translucent, color.opacity(0.3));
                assert_eq!(cx.window_theme(window).colors().editor_background.a, 0.0);
                assert_eq!(
                    cx.window_theme(window).colors().text,
                    original.colors().text
                );
                assert_eq!(
                    cx.window_theme(window).colors().elevated_surface_background,
                    original.colors().elevated_surface_background
                );
                assert_eq!(
                    cx.window_theme(window).colors().element_background,
                    original.colors().element_background
                );
            })
            .expect("Media window update");
        normal_window
            .update(cx, |_, window, cx| {
                assert!(Arc::ptr_eq(cx.window_theme(window), cx.theme()));
            })
            .expect("Normal window update");
        media_window
            .update(cx, |_, window, cx| {
                set_media_background(window.window_handle().window_id(), false, cx);
                assert!(Arc::ptr_eq(cx.window_theme(window), cx.theme()));
            })
            .expect("Restore window theme");
    }
}
