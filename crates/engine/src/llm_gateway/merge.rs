use serde_json::Value;

pub(crate) fn merge_event_extra(event: &mut Value, extra: Value) {
    let (Some(event), Some(extra)) = (event.as_object_mut(), extra.as_object()) else {
        return;
    };
    for (key, value) in extra {
        event.insert(key.clone(), value.clone());
    }
}
