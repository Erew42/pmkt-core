use super::*;

pub fn decode(message: &Value) -> Result<Event> {
    let kind = message["event_type"]
        .as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| message["type"].as_str());
    let market = text(message.get("market")).filter(|s| !s.is_empty());
    let asset = text(message.get("asset_id")).filter(|s| !s.is_empty());
    let timestamp = SourceTime {
        value: message.get("timestamp").filter(|v| !v.is_null()).cloned(),
        milliseconds: true,
    };
    let update = |instrument, action| Update {
        instrument,
        market: market.clone(),
        timestamp: timestamp.clone(),
        sequence: None,
        action,
    };
    Ok(match kind {
        Some("book") => Event::Books(vec![update(
            asset.ok_or_else(|| Error::Invalid("missing_book_identity".into()))?,
            Action::Snapshot(Snapshot {
                first: levels(message.get("bids"), false),
                second: levels(message.get("asks"), false),
                hash: text(message.get("hash")),
                tick: Metadata::read(message.get("tick_size")),
                minimum: Metadata::read(message.get("min_order_size")),
            }),
        )]),
        Some("price_change") => {
            let invalid = || Error::Invalid("malformed_price_changes".into());
            let changes = message["price_changes"].as_array().ok_or_else(invalid)?;
            let mut updates = Vec::with_capacity(changes.len());
            for change in changes {
                let instrument = text(change.get("asset_id"))
                    .filter(|s| !s.is_empty())
                    .ok_or_else(invalid)?;
                let side = change["side"].as_str().unwrap_or("");
                let previous_hash = [change, message].into_iter().find_map(|obj| {
                    [
                        "previous_hash",
                        "prev_hash",
                        "old_hash",
                        "previous_book_hash",
                    ]
                    .into_iter()
                    .find_map(|k| text(obj.get(k)))
                });
                let mut u = update(
                    instrument,
                    Action::Absolute(Change {
                        side: match side.to_ascii_uppercase().as_str() {
                            "BUY" | "BID" | "BIDS" => Some(Side::First),
                            "SELL" | "ASK" | "ASKS" => Some(Side::Second),
                            _ => None,
                        },
                        price: price(change.get("price")),
                        size: decimal(change.get("size")),
                        hash: text(change.get("hash")),
                        previous_hash,
                        corrective_side: matches!(side, "BUY" | "SELL"),
                        parent_timestamp: text(
                            message.get("timestamp").filter(|v| v.as_i64() != Some(0)),
                        ),
                        reported_bid: price(change.get("best_bid")),
                        reported_ask: price(change.get("best_ask")),
                    }),
                );
                if u.timestamp.value.is_none() {
                    u.timestamp.value = change.get("timestamp").cloned();
                }
                updates.push(u);
            }
            Event::Books(updates)
        }
        Some("tick_size_change") => Event::Books(
            asset
                .into_iter()
                .map(|a| update(a, Action::Tick(decimal(message.get("new_tick_size")))))
                .collect(),
        ),
        Some("market_resolved" | "new_market") => {
            let assets = message["assets_ids"]
                .as_array()
                .filter(|a| !a.is_empty())
                .or_else(|| message["asset_ids"].as_array());
            let instruments = asset
                .into_iter()
                .chain(
                    assets
                        .into_iter()
                        .flatten()
                        .filter_map(|v| v.as_str().map(str::to_owned)),
                )
                .collect();
            Event::Lifecycle {
                market,
                instruments,
                resolved: kind == Some("market_resolved"),
                timestamp,
            }
        }
        Some("error") => Event::SubscriptionError,
        _ => Event::Other,
    })
}
