use std::collections::HashSet;

use acp_thread::{AcpThread, AcpThreadEvent, ThreadStatus};
use agent_settings::AgentSettings;
use gpui::{App, BorrowAppContext as _, EntityId, Global, ReadGlobal as _, SystemSleepPrevention};
use settings::{Settings as _, SettingsStore};

struct AgentSystemSleepPrevention {
    active_threads: HashSet<EntityId>,
    prevention: Option<SystemSleepPrevention>,
}

impl Global for AgentSystemSleepPrevention {}

pub fn init(cx: &mut App) {
    cx.set_global(AgentSystemSleepPrevention {
        active_threads: HashSet::new(),
        prevention: None,
    });

    cx.observe_global::<SettingsStore>(refresh).detach();
    cx.observe_new::<AcpThread>(|thread, _window, cx| {
        let entity_id = cx.entity_id();
        update_thread(entity_id, is_active(thread), cx);

        cx.subscribe_self(move |thread, _event: &AcpThreadEvent, cx| {
            update_thread(entity_id, is_active(thread), cx);
        })
        .detach();

        cx.on_release(move |_thread, cx| {
            update_thread(entity_id, false, cx);
        })
        .detach();
    })
    .detach();
}

fn is_active(thread: &AcpThread) -> bool {
    thread.status() == ThreadStatus::Generating && !thread.is_waiting_for_confirmation()
}

fn update_thread(entity_id: EntityId, active: bool, cx: &mut App) {
    cx.update_global::<AgentSystemSleepPrevention, _>(|state, cx| {
        if active {
            state.active_threads.insert(entity_id);
        } else {
            state.active_threads.remove(&entity_id);
        }
        state.refresh(cx);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use gpui::{AppContext as _, TestAppContext, UpdateGlobal as _};
    use project::DisableAiSettings;

    #[gpui::test]
    fn prevents_sleep_until_all_active_threads_are_waiting(cx: &mut TestAppContext) {
        cx.update(|cx| {
            let settings_store = SettingsStore::test(cx);
            cx.set_global(settings_store);
            DisableAiSettings::register(cx);
            AgentSettings::register(cx);
            SettingsStore::update_global(cx, |store, cx| {
                let settings = store
                    .new_text_for_update("{}".to_string(), |settings| {
                        settings
                            .agent
                            .get_or_insert_default()
                            .prevent_system_sleep_when_running = Some(true);
                    })
                    .expect("test settings update should succeed");
                store
                    .set_user_settings(&settings, cx)
                    .expect("test settings should be valid");
            });
            init(cx);
        });

        let first_thread = cx.new(|_| ());
        let second_thread = cx.new(|_| ());

        cx.update(|cx| update_thread(first_thread.entity_id(), true, cx));
        assert_eq!(cx.system_sleep_prevention_count(), 1);

        cx.update(|cx| update_thread(second_thread.entity_id(), true, cx));
        assert_eq!(cx.system_sleep_prevention_count(), 1);

        cx.update(|cx| update_thread(first_thread.entity_id(), false, cx));
        assert_eq!(cx.system_sleep_prevention_count(), 1);

        cx.update(|cx| update_thread(second_thread.entity_id(), false, cx));
        assert_eq!(cx.system_sleep_prevention_count(), 0);

        cx.update(|cx| update_thread(first_thread.entity_id(), true, cx));
        assert_eq!(cx.system_sleep_prevention_count(), 1);

        cx.update(|cx| {
            SettingsStore::update_global(cx, |store, cx| {
                let settings = store
                    .new_text_for_update("{}".to_string(), |settings| {
                        settings
                            .agent
                            .get_or_insert_default()
                            .prevent_system_sleep_when_running = Some(false);
                    })
                    .expect("test settings update should succeed");
                store
                    .set_user_settings(&settings, cx)
                    .expect("test settings should be valid");
            });
        });
        assert_eq!(cx.system_sleep_prevention_count(), 0);
    }
}

fn refresh(cx: &mut App) {
    cx.update_global::<AgentSystemSleepPrevention, _>(|state, cx| state.refresh(cx));
}

impl AgentSystemSleepPrevention {
    fn refresh(&mut self, cx: &App) {
        let should_prevent_sleep = AgentSettings::get_global(cx).prevent_system_sleep_when_running
            && !self.active_threads.is_empty();

        if should_prevent_sleep && self.prevention.is_none() {
            self.prevention = cx.prevent_system_sleep("Zed Agent is actively working");
        } else if !should_prevent_sleep {
            self.prevention = None;
        }
    }
}
