use crate::{DismissDecision, ModalView, Workspace};
use gpui::{DismissEvent, Entity, EventEmitter, FocusHandle, Focusable, Global, Task, actions};
use ui::{AlertModal, prelude::*};
use ui_input::InputField;
use util::ResultExt as _;

const KEYCHAIN_SERVICE: &str = "zed.background-media.holodex";

actions!(
    background_media,
    [
        /// Stores the Holodex API key in the operating system's credential store.
        SetHolodexApiKey,
        /// Removes the Holodex API key from the operating system's credential store.
        RemoveHolodexApiKey,
    ]
);

#[derive(Default)]
pub(crate) struct HolodexKey {
    pub value: Option<Result<Option<String>, String>>,
    pub revision: u64,
    loading: Option<Task<()>>,
}
impl Global for HolodexKey {}

pub(crate) fn init(cx: &mut App) {
    cx.observe_new(|workspace: &mut Workspace, _, _| {
        workspace.register_action(|workspace, _: &SetHolodexApiKey, window, cx| {
            workspace.toggle_modal(window, cx, |window, cx| {
                HolodexKeyModal::new(false, window, cx)
            });
        });
        workspace.register_action(|workspace, _: &RemoveHolodexApiKey, window, cx| {
            workspace.toggle_modal(window, cx, |window, cx| {
                HolodexKeyModal::new(true, window, cx)
            });
        });
    })
    .detach();
}

fn validated_key(key: &str) -> Result<String, String> {
    let key = key.trim();
    if key.is_empty() || key.len() > 4096 || !key.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err("Enter a nonempty API key without whitespace or control characters".into());
    }
    Ok(key.to_owned())
}

fn environment_key() -> Result<Option<String>, String> {
    match std::env::var("HOLODEX_API_KEY") {
        Ok(key) if !key.trim().is_empty() => validated_key(&key).map(Some),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err("HOLODEX_API_KEY is not valid text".into()),
    }
}

fn resolve_key(
    stored: anyhow::Result<Option<(String, Vec<u8>)>>,
    fallback: impl FnOnce() -> Result<Option<String>, String>,
) -> Result<Option<String>, String> {
    match stored {
        Ok(Some((_, bytes))) => String::from_utf8(bytes)
            .map_err(|_| "Stored Holodex key is not valid text".to_owned())
            .and_then(|key| validated_key(&key).map(Some)),
        Ok(None) => fallback(),
        Err(error) => Err(format!(
            "Cannot read Holodex key from the system credential store: {error}"
        )),
    }
}

pub(crate) fn ensure_loaded(cx: &mut App) {
    if !cx.has_global::<HolodexKey>() {
        cx.set_global(HolodexKey::default());
    }
    let state = cx.global::<HolodexKey>();
    if state.value.is_some() || state.loading.is_some() {
        return;
    }
    let revision = state.revision;
    // The higher-level development provider writes plaintext; use the native platform store directly.
    let read = cx.read_credentials(KEYCHAIN_SERVICE);
    let task = cx.spawn(async move |cx| {
        let value = resolve_key(read.await, environment_key);
        cx.update(|cx| {
            let state = cx.global_mut::<HolodexKey>();
            if state.revision == revision {
                state.value = Some(value);
                state.revision += 1;
            }
        });
    });
    cx.global_mut::<HolodexKey>().loading = Some(task);
}

fn publish(value: Result<Option<String>, String>, cx: &mut App) {
    if !cx.has_global::<HolodexKey>() {
        cx.set_global(HolodexKey::default());
    }
    let state = cx.global_mut::<HolodexKey>();
    state.loading = None;
    state.value = Some(value);
    state.revision += 1;
}

struct HolodexKeyModal {
    input: Entity<InputField>,
    remove: bool,
    busy: bool,
    error: Option<String>,
    task: Option<Task<()>>,
}

impl HolodexKeyModal {
    fn new(remove: bool, window: &mut Window, cx: &mut Context<Self>) -> Self {
        let input = cx.new(|cx| InputField::new(window, cx, "Holodex API key").masked(true));
        input.update(cx, |input, cx| input.set_masked(true, window, cx));
        Self {
            input,
            remove,
            busy: false,
            error: None,
            task: None,
        }
    }

    fn confirm(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let key = if self.remove {
            None
        } else {
            match validated_key(&self.input.read(cx).text(cx)) {
                Ok(key) => Some(key),
                Err(error) => {
                    self.error = Some(error);
                    cx.notify();
                    return;
                }
            }
        };
        let operation = if let Some(key) = &key {
            cx.write_credentials(KEYCHAIN_SERVICE, "api-key", key.as_bytes())
        } else {
            cx.delete_credentials(KEYCHAIN_SERVICE)
        };
        self.busy = true;
        self.error = None;
        cx.notify();
        self.task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = operation.await;
            this.update_in(cx, |this, window, cx| {
                this.busy = false;
                match result {
                    Ok(()) => {
                        publish(key.map_or_else(environment_key, |key| Ok(Some(key))), cx);
                        this.input.update(cx, |input, cx| input.clear(window, cx));
                        cx.emit(DismissEvent);
                    }
                    Err(error) => {
                        this.error = Some(format!(
                            "Could not update the system credential store: {error}"
                        ));
                        cx.notify();
                    }
                }
            })
            .log_err();
        }));
    }
}

impl EventEmitter<DismissEvent> for HolodexKeyModal {}
impl Focusable for HolodexKeyModal {
    fn focus_handle(&self, cx: &App) -> FocusHandle {
        self.input.focus_handle(cx)
    }
}
impl ModalView for HolodexKeyModal {
    fn on_before_dismiss(&mut self, _: &mut Window, _: &mut Context<Self>) -> DismissDecision {
        DismissDecision::Dismiss(!self.busy)
    }
}
impl Render for HolodexKeyModal {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        AlertModal::new("holodex-api-key")
            .width(rems(30.))
            .track_focus(&self.focus_handle(cx))
            .on_action(cx.listener(|this, _: &menu::Confirm, window, cx| this.confirm(window, cx)))
            .on_action(cx.listener(|this, _: &menu::Cancel, _, cx| {
                if !this.busy { cx.emit(DismissEvent); }
            }))
            .title(if self.remove { "Remove Holodex API Key" } else { "Set Holodex API Key" })
            .child(v_flex().p_3().gap_3()
                .child(Label::new(if self.remove {
                    "Remove the stored key? HOLODEX_API_KEY remains a fallback if set."
                } else {
                    "Stored in the system credential store (Login Keychain on macOS), not settings.json."
                }))
                .when(!self.remove, |this| this.child(self.input.clone()))
                .when_some(self.error.clone(), |this, error| this.child(Label::new(error).color(Color::Error)))
                .child(h_flex().justify_end().gap_2()
                    .child(Button::new("cancel", "Cancel").disabled(self.busy).on_click(cx.listener(|_, _, _, cx| cx.emit(DismissEvent))))
                    .child(Button::new("confirm", if self.remove { "Remove" } else { "Save" }).disabled(self.busy)
                        .on_click(cx.listener(|this, _, window, cx| this.confirm(window, cx))))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_keys_without_disclosing_them() {
        assert_eq!(validated_key("  test-key  "), Ok("test-key".into()));
        for key in ["", "\n", "secret\r\nheader", "secret key", "秘密"] {
            let error = validated_key(key).expect_err("Invalid key should be rejected");
            assert!(!error.contains("secret"));
        }
    }

    #[test]
    fn stored_key_precedes_environment_and_store_errors_do_not_fall_back() {
        assert_eq!(
            resolve_key(
                Ok(Some(("api-key".into(), b"stored-key".to_vec()))),
                || panic!("Environment should not be read")
            ),
            Ok(Some("stored-key".into()))
        );
        assert_eq!(
            resolve_key(Ok(None), || Ok(Some("environment-key".into()))),
            Ok(Some("environment-key".into()))
        );
        assert!(
            resolve_key(Err(anyhow::anyhow!("Access denied")), || panic!(
                "Store errors must not fall back"
            ))
            .is_err()
        );
        assert!(
            resolve_key(Ok(Some(("api-key".into(), vec![255]))), || panic!(
                "Invalid stored keys must not fall back"
            ))
            .is_err()
        );
    }

    #[gpui::test(iterations = 10)]
    fn replacing_key_cancels_stale_load(cx: &mut gpui::TestAppContext) {
        cx.update(|cx| {
            ensure_loaded(cx);
            publish(Ok(Some("new-key".into())), cx);
        });
        cx.run_until_parked();
        cx.update(|cx| {
            ensure_loaded(cx);
            assert_eq!(
                cx.global::<HolodexKey>().value,
                Some(Ok(Some("new-key".into())))
            );
            assert_eq!(cx.global::<HolodexKey>().revision, 1);
        });
    }

    #[gpui::test]
    fn removing_key_invalidates_all_players(cx: &mut gpui::TestAppContext) {
        cx.update(|cx| {
            publish(Ok(Some("old-key".into())), cx);
            let revision = cx.global::<HolodexKey>().revision;
            publish(Ok(None), cx);
            assert_eq!(cx.global::<HolodexKey>().value, Some(Ok(None)));
            assert_eq!(cx.global::<HolodexKey>().revision, revision + 1);
        });
    }
}
