use crate::{
    App, Bounds, Element, ElementId, GlobalElementId, InspectorElementId, IntoElement, LayoutId,
    ObjectFit, Pixels, Style, StyleRefinement, Styled, Window,
};
#[cfg(target_os = "macos")]
use core_video::pixel_buffer::CVPixelBuffer;
use refineable::Refineable;
use std::sync::Arc;

/// An immutable, tightly packed, full-range BT.709 NV12 video frame.
#[derive(Clone, Debug)]
pub struct Nv12Frame {
    /// Unique frame identity, used to avoid uploading an unchanged frame.
    pub id: u64,
    /// Frame width, in physical pixels.
    pub width: u32,
    /// Frame height, in physical pixels.
    pub height: u32,
    /// Y plane followed by interleaved UV, both with a row stride equal to width.
    pub data: Arc<[u8]>,
}

impl Nv12Frame {
    /// Validates plane lengths before exposing them to a graphics API.
    pub fn new(id: u64, width: u32, height: u32, data: Arc<[u8]>) -> anyhow::Result<Self> {
        anyhow::ensure!(
            width > 0 && height > 0 && width.is_multiple_of(2) && height.is_multiple_of(2),
            "NV12 dimensions must be positive and even"
        );
        let bytes = (width as usize)
            .checked_mul(height as usize)
            .and_then(|pixels| pixels.checked_mul(3))
            .map(|bytes| bytes / 2);
        anyhow::ensure!(bytes == Some(data.len()), "Invalid NV12 plane lengths");
        Ok(Self {
            id,
            width,
            height,
            data,
        })
    }
}

impl PartialEq for Nv12Frame {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}
impl Eq for Nv12Frame {}

/// A source of a surface's content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SurfaceSource {
    /// CPU-backed NV12 frame, uploaded once per renderer.
    Nv12(Arc<Nv12Frame>),
    /// A macOS image buffer from CoreVideo
    #[cfg(target_os = "macos")]
    Surface(CVPixelBuffer),
}

impl From<Arc<Nv12Frame>> for SurfaceSource {
    fn from(value: Arc<Nv12Frame>) -> Self {
        Self::Nv12(value)
    }
}

#[cfg(target_os = "macos")]
impl From<CVPixelBuffer> for SurfaceSource {
    fn from(value: CVPixelBuffer) -> Self {
        SurfaceSource::Surface(value)
    }
}

/// A surface element.
pub struct Surface {
    source: SurfaceSource,
    object_fit: ObjectFit,
    style: StyleRefinement,
}

/// Create a new surface element.
pub fn surface(source: impl Into<SurfaceSource>) -> Surface {
    Surface {
        source: source.into(),
        object_fit: ObjectFit::Contain,
        style: Default::default(),
    }
}

impl Surface {
    /// Set the object fit for the image.
    pub fn object_fit(mut self, object_fit: ObjectFit) -> Self {
        self.object_fit = object_fit;
        self
    }
}

impl Element for Surface {
    type RequestLayoutState = ();
    type PrepaintState = ();

    fn id(&self) -> Option<ElementId> {
        None
    }

    fn source_location(&self) -> Option<&'static core::panic::Location<'static>> {
        None
    }

    fn request_layout(
        &mut self,
        _global_id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> (LayoutId, Self::RequestLayoutState) {
        let mut style = Style::default();
        style.refine(&self.style);
        let layout_id = window.request_layout(style, [], cx);
        (layout_id, ())
    }

    fn prepaint(
        &mut self,
        _global_id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        _bounds: Bounds<Pixels>,
        _request_layout: &mut Self::RequestLayoutState,
        _window: &mut Window,
        _cx: &mut App,
    ) -> Self::PrepaintState {
    }

    fn paint(
        &mut self,
        _global_id: Option<&GlobalElementId>,
        _inspector_id: Option<&InspectorElementId>,
        #[cfg_attr(not(target_os = "macos"), allow(unused_variables))] bounds: Bounds<Pixels>,
        _: &mut Self::RequestLayoutState,
        _: &mut Self::PrepaintState,
        #[cfg_attr(not(target_os = "macos"), allow(unused_variables))] window: &mut Window,
        _: &mut App,
    ) {
        match &self.source {
            SurfaceSource::Nv12(frame) => {
                let size = crate::size(
                    crate::DevicePixels(frame.width as i32),
                    crate::DevicePixels(frame.height as i32),
                );
                let new_bounds = self.object_fit.get_bounds(bounds, size);
                window.paint_video_surface(new_bounds, self.source.clone());
            }
            #[cfg(target_os = "macos")]
            SurfaceSource::Surface(surface) => {
                let size = crate::size(surface.get_width().into(), surface.get_height().into());
                let new_bounds = self.object_fit.get_bounds(bounds, size);
                // TODO: Add support for corner_radii
                window.paint_surface(new_bounds, surface.clone());
            }
            #[allow(unreachable_patterns)]
            _ => {}
        }
    }
}

impl IntoElement for Surface {
    type Element = Self;

    fn into_element(self) -> Self::Element {
        self
    }
}

impl Styled for Surface {
    fn style(&mut self) -> &mut StyleRefinement {
        &mut self.style
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nv12_rejects_invalid_plane_lengths_and_dimensions() {
        assert!(Nv12Frame::new(1, 2, 2, Arc::from([0; 6])).is_ok());
        assert!(Nv12Frame::new(1, 2, 2, Arc::from([0; 5])).is_err());
        assert!(Nv12Frame::new(1, 3, 2, Arc::from([0; 9])).is_err());
        assert!(Nv12Frame::new(1, 0, 2, Arc::from([])).is_err());
    }
}
