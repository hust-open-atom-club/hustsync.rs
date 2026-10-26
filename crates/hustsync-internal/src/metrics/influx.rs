//! InfluxDB line protocol in the shape Telegraf's `inputs.prometheus` writes with `metric_version = 2`,
//! so pushed and scraped metrics come out of Telegraf alike.

use std::time::{SystemTime, UNIX_EPOCH};

use influxdb_line_protocol::LineProtocolBuilder;
use prometheus::proto::{MetricFamily, MetricType};

use super::Families;

type Builder = LineProtocolBuilder<Vec<u8>>;

struct Line<'a> {
    tags: Vec<(&'a str, String)>,
    fields: Vec<(String, f64)>,
    ns: i64,
}

/// Every metric of the groups, one line per sample, stamped `at` unless a metric carries its own time.
pub fn encode(groups: &[Families], at: SystemTime) -> Vec<u8> {
    let ns = at
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_nanos()).ok())
        .unwrap_or_default();
    groups
        .iter()
        .flat_map(|g| g.iter())
        .flat_map(|f| lines(f, ns))
        .fold(Builder::new(), write)
        .build()
}

fn lines(f: &MetricFamily, ns: i64) -> Vec<Line<'_>> {
    let name = f.name();
    let mut out = Vec::new();
    for m in f.get_metric() {
        let ns = match m.timestamp_ms() {
            0 => ns,
            ms => ms.saturating_mul(1_000_000),
        };
        let tags: Vec<_> = m
            .get_label()
            .iter()
            .filter(|l| !l.value().is_empty())
            .map(|l| (l.name(), l.value().to_owned()))
            .collect();
        let mut line = |extra: Option<(&'static str, String)>, fields: Vec<(String, f64)>| {
            let mut tags = tags.clone();
            tags.extend(extra);
            out.push(Line { tags, fields, ns });
        };
        match f.get_field_type() {
            MetricType::COUNTER => line(None, vec![(name.into(), m.get_counter().get_value())]),
            MetricType::GAUGE => line(None, vec![(name.into(), m.get_gauge().get_value())]),
            MetricType::UNTYPED => {}
            MetricType::HISTOGRAM => {
                let h = m.get_histogram();
                let n = h.get_sample_count();
                line(
                    None,
                    vec![
                        (format!("{name}_count"), n as f64),
                        (format!("{name}_sum"), h.get_sample_sum()),
                    ],
                );
                let inf = h
                    .get_bucket()
                    .last()
                    .is_some_and(|b| b.upper_bound().is_infinite());
                let buckets = h
                    .get_bucket()
                    .iter()
                    .map(|b| (b.upper_bound(), b.cumulative_count()))
                    .chain((!inf).then_some((f64::INFINITY, n)));
                for (le, c) in buckets {
                    line(
                        Some(("le", go(le))),
                        vec![(format!("{name}_bucket"), c as f64)],
                    );
                }
            }
            MetricType::SUMMARY => {
                let s = m.get_summary();
                line(
                    None,
                    vec![
                        (format!("{name}_sum"), s.sample_sum()),
                        (format!("{name}_count"), s.sample_count() as f64),
                    ],
                );
                for q in s.get_quantile() {
                    line(
                        Some(("quantile", go(q.quantile()))),
                        vec![(name.into(), q.value())],
                    );
                }
            }
        }
    }
    out
}

fn write(lp: Builder, mut l: Line<'_>) -> Builder {
    l.fields.retain(|(_, v)| v.is_finite());
    let Some(((key, value), rest)) = l.fields.split_first() else {
        return lp;
    };
    l.tags.sort_unstable_by_key(|t| t.0);
    let lp = l
        .tags
        .iter()
        .fold(lp.measurement("prometheus"), |lp, (k, v)| lp.tag(k, v));
    rest.iter()
        .fold(lp.field(key, *value), |lp, (k, v)| lp.field(k, *v))
        .timestamp(l.ns)
        .close_line()
}

/// A float as Go's `fmt.Sprint` writes it, which is how Telegraf spells `le` and `quantile`.
fn go(v: f64) -> String {
    if v.is_nan() {
        return "NaN".into();
    }
    if v.is_infinite() {
        return if v > 0.0 { "+Inf" } else { "-Inf" }.into();
    }
    let e = format!("{v:e}");
    let Some((mant, exp)) = e.split_once('e') else {
        return e;
    };
    let exp: i32 = exp.parse().unwrap_or_default();
    if v == 0.0 || (-4..6).contains(&exp) {
        return format!("{v}");
    }
    let sign = if exp < 0 { '-' } else { '+' };
    format!("{mant}e{sign}{:02}", exp.abs())
}
