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

pub(super) fn body(message: &Value) -> &Value {
    message
        .get("msg")
        .filter(|v| v.is_object())
        .unwrap_or(message)
}

pub const DELTA_PRICE_FIELDS: [&str; 2] = ["price_dollars", "price"];
pub const DELTA_SIZE_FIELDS: [&str; 3] = ["delta_fp", "delta", "size_delta"];

pub fn relative(side: &str, price: Option<D>, delta: Option<D>) -> Action {
    Action::Relative {
        side: match side.to_ascii_lowercase().as_str() {
            "yes" => Some(Side::First),
            "no" => Some(Side::Second),
            _ => None,
        },
        price: price.filter(|p| p.is_probability()),
        delta,
    }
}

pub fn decode(message: &Value) -> Result<Event> {
    let kind = message["type"].as_str().unwrap_or("");
    let body = body(message);
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
        Action::Snapshot(snapshot(Venue::Kalshi, body))
    } else {
        relative(
            body["side"].as_str().unwrap_or(""),
            decimal(first(body, &DELTA_PRICE_FIELDS)),
            decimal(first(body, &DELTA_SIZE_FIELDS)),
        )
    };
    Ok(Event::Books(vec![Update {
        instrument,
        market: text(body.get("market_id")),
        timestamp,
        sequence,
        action,
    }]))
}
