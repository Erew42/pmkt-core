use super::*;

/// Price basis applies only to order-book subscriptions; another channel's
/// command must not change how later depth is interpreted.
pub(crate) fn subscription_basis(command: &Value) -> Result<Option<bool>> {
    let params = &command["params"];
    if !params["channels"]
        .as_array()
        .is_some_and(|cs| cs.iter().any(|c| c == "orderbook_delta"))
    {
        return Ok(None);
    }
    params.get("use_yes_price").map_or(Ok(Some(true)), |v| {
        v.as_bool()
            .map(Some)
            .ok_or_else(|| Error::Invalid("use_yes_price must be boolean".into()))
    })
}

pub fn decode(message: &Value) -> Result<Event> {
    let kind = message["type"].as_str().unwrap_or("");
    let body = message
        .get("msg")
        .filter(|v| v.is_object())
        .unwrap_or(message);
    match kind {
        "subscribed" => {
            return Ok(Event::Subscribed {
                command_id: message["id"].as_i64(),
                sid: message["msg"]["sid"].as_i64(),
                book_channel: message["msg"]["channel"] == "orderbook_delta",
            });
        }
        "error" => return Ok(Event::SubscriptionError),
        "orderbook_snapshot" | "orderbook_delta" => {}
        _ => return Ok(Event::Other),
    }
    let instrument = text(body.get("market_ticker"))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Invalid("invalid_book_identity_or_sequence".into()))?;
    let ms = body.get("ts_ms").filter(|v| !v.is_null());
    let timestamp = SourceTime {
        value: ms.or_else(|| body.get("ts")).cloned(),
        milliseconds: ms.is_some(),
    };
    let sequence = Some(Sequence {
        sid: message["sid"].as_i64(),
        seq: message["seq"].as_i64(),
    });
    let action = if kind == "orderbook_snapshot" {
        Action::Snapshot(Snapshot {
            first: levels(first(body, &["yes_dollars_fp", "yes_dollars", "yes"]), true),
            second: levels(first(body, &["no_dollars_fp", "no_dollars", "no"]), true),
            hash: None,
            tick: Metadata::Absent,
            minimum: Metadata::Absent,
        })
    } else {
        Action::Relative {
            side: match body["side"]
                .as_str()
                .unwrap_or("")
                .to_ascii_lowercase()
                .as_str()
            {
                "yes" => Some(Side::First),
                "no" => Some(Side::Second),
                _ => None,
            },
            price: price(first(body, &["price_dollars", "price"])),
            delta: decimal(first(body, &["delta_fp", "delta", "size_delta"])),
        }
    };
    Ok(Event::Books(vec![Update {
        instrument,
        market: text(body.get("market_id")),
        timestamp,
        sequence,
        action,
    }]))
}
