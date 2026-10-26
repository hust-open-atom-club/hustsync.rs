//! OTLP/HTTP protobuf bodies for `/v1/metrics`.

use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::{
    collector::metrics::v1::ExportMetricsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric as Point,
        NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint, metric,
        number_data_point, summary_data_point::ValueAtQuantile,
    },
    resource::v1::Resource,
};
use prometheus::proto::{LabelPair, Metric, MetricFamily, MetricType};

use super::{Families, push::Origin};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// An `ExportMetricsServiceRequest` holding every metric of the groups.
pub fn encode(groups: &[Families], origin: &Origin, at: SystemTime) -> ExportMetricsServiceRequest {
    let span = Span {
        start: nanos(origin.start),
        now: nanos(at),
    };
    let resource = Resource {
        attributes: attrs([
            ("service.name", origin.service),
            ("service.instance.id", &origin.instance),
            ("service.version", VERSION),
        ]),
        ..Resource::default()
    };
    let scope = ScopeMetrics {
        scope: Some(InstrumentationScope {
            name: "hustsync".into(),
            version: VERSION.into(),
            ..InstrumentationScope::default()
        }),
        metrics: groups
            .iter()
            .flat_map(|g| g.iter())
            .filter_map(|f| family(f, span))
            .collect(),
        ..ScopeMetrics::default()
    };
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(resource),
            scope_metrics: vec![scope],
            ..ResourceMetrics::default()
        }],
    }
}

#[derive(Clone, Copy)]
struct Span {
    start: u64,
    now: u64,
}

impl Span {
    fn at(self, m: &Metric) -> u64 {
        match u64::try_from(m.timestamp_ms()) {
            Ok(0) | Err(_) => self.now,
            Ok(ms) => ms.saturating_mul(1_000_000),
        }
    }
}

fn family(f: &MetricFamily, span: Span) -> Option<Point> {
    let ms = f.get_metric();
    let numbers = |value: fn(&Metric) -> f64| {
        ms.iter()
            .filter(|m| value(m).is_finite())
            .map(|m| NumberDataPoint {
                attributes: labels(m.get_label()),
                start_time_unix_nano: span.start,
                time_unix_nano: span.at(m),
                value: Some(number_data_point::Value::AsDouble(value(m))),
                ..NumberDataPoint::default()
            })
            .collect()
    };
    let cumulative = AggregationTemporality::Cumulative.into();
    let (name, data) = match f.get_field_type() {
        MetricType::UNTYPED => return None,
        MetricType::GAUGE => (
            f.name(),
            metric::Data::Gauge(Gauge {
                data_points: numbers(|m| m.get_gauge().get_value()),
            }),
        ),
        MetricType::COUNTER => (
            f.name().trim_end_matches("_total"),
            metric::Data::Sum(Sum {
                data_points: numbers(|m| m.get_counter().get_value()),
                aggregation_temporality: cumulative,
                is_monotonic: true,
            }),
        ),
        MetricType::HISTOGRAM => (
            f.name(),
            metric::Data::Histogram(Histogram {
                data_points: ms.iter().map(|m| histogram(m, span)).collect(),
                aggregation_temporality: cumulative,
            }),
        ),
        MetricType::SUMMARY => (
            f.name(),
            metric::Data::Summary(Summary {
                data_points: ms.iter().map(|m| summary(m, span)).collect(),
            }),
        ),
    };
    Some(Point {
        name: name.into(),
        description: f.help().into(),
        data: Some(data),
        ..Point::default()
    })
}

fn histogram(m: &Metric, span: Span) -> HistogramDataPoint {
    let h = m.get_histogram();
    let finite: Vec<_> = h
        .get_bucket()
        .iter()
        .filter(|b| b.upper_bound().is_finite())
        .collect();
    let mut seen = 0;
    let mut bucket_counts: Vec<u64> = finite
        .iter()
        .map(|b| {
            let n = b.cumulative_count() - seen;
            seen = b.cumulative_count();
            n
        })
        .collect();
    bucket_counts.push(h.get_sample_count() - seen);
    HistogramDataPoint {
        attributes: labels(m.get_label()),
        start_time_unix_nano: span.start,
        time_unix_nano: span.at(m),
        count: h.get_sample_count(),
        sum: Some(h.get_sample_sum()),
        bucket_counts,
        explicit_bounds: finite.iter().map(|b| b.upper_bound()).collect(),
        ..HistogramDataPoint::default()
    }
}

fn summary(m: &Metric, span: Span) -> SummaryDataPoint {
    let s = m.get_summary();
    SummaryDataPoint {
        attributes: labels(m.get_label()),
        start_time_unix_nano: span.start,
        time_unix_nano: span.at(m),
        count: s.sample_count(),
        sum: s.sample_sum(),
        quantile_values: s
            .get_quantile()
            .iter()
            .map(|q| ValueAtQuantile {
                quantile: q.quantile(),
                value: q.value(),
            })
            .collect(),
        ..SummaryDataPoint::default()
    }
}

fn labels(ls: &[LabelPair]) -> Vec<KeyValue> {
    attrs(ls.iter().map(|l| (l.name(), l.value())))
}

fn attrs<'a>(kv: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<KeyValue> {
    kv.into_iter()
        .filter(|(_, v)| !v.is_empty())
        .map(|(k, v)| KeyValue {
            key: k.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.into())),
            }),
            ..KeyValue::default()
        })
        .collect()
}

fn nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_nanos()).ok())
        .unwrap_or_default()
}
