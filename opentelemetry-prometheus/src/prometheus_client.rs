//! Exports metrics through a [`prometheus_client`] registry, including
//! exponential histograms as Prometheus native histograms.

use std::{
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex},
};

use ::prometheus_client::{
    collector::Collector as ClientCollectorTrait,
    encoding::{
        DescriptorEncoder, MetricEncoder, NativeHistogram, NativeHistogramBuckets, NoLabelSet,
    },
    metrics::MetricType as ClientMetricType,
};
use opentelemetry::otel_error;
use opentelemetry_sdk::metrics::{
    data::{self, ResourceMetrics},
    reader::MetricReader,
    ManualReader,
};
use prometheus::proto::{LabelPair, MetricFamily, MetricType};

use crate::{
    get_attrs, get_scope_labels, validate_metrics, Collector, Numeric, COUNTER_SUFFIX,
    TARGET_INFO_DESCRIPTION, TARGET_INFO_NAME,
};

/// The upper bound that `prometheus_client` encodes as `+Inf`.
const INF: f64 = f64::MAX;

/// Name, help, type, and the metrics in the family with their scope labels.
type Family<'a> = (
    String,
    String,
    MetricType,
    Vec<(&'a data::Metric, Vec<LabelPair>)>,
);

pub(crate) struct ClientCollector {
    collector: Collector,
    families: Mutex<HashMap<String, MetricFamily>>,
}

impl fmt::Debug for ClientCollector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCollector").finish_non_exhaustive()
    }
}

impl ClientCollector {
    pub(crate) fn new(collector: Collector) -> Self {
        ClientCollector {
            collector,
            families: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn reader(&self) -> Arc<ManualReader> {
        Arc::clone(&self.collector.reader)
    }

    fn metric_type_and_name(&self, m: &data::Metric) -> Option<(MetricType, String)> {
        let (metric_type, name) = match m.data() {
            data::AggregatedMetrics::F64(data::MetricData::ExponentialHistogram(_))
            | data::AggregatedMetrics::I64(data::MetricData::ExponentialHistogram(_))
            | data::AggregatedMetrics::U64(data::MetricData::ExponentialHistogram(_)) => {
                (MetricType::HISTOGRAM, self.collector.get_name(m))
            }
            _ => self.collector.metric_type_and_name(m)?,
        };
        Some((metric_type, name.into_owned()))
    }
}

impl ClientCollectorTrait for ClientCollector {
    fn encode(&self, mut encoder: DescriptorEncoder<'_>) -> Result<(), fmt::Error> {
        let mut metrics = ResourceMetrics::default();
        if let Err(err) = self.collector.reader.collect(&mut metrics) {
            otel_error!(name: "MetricScrapeFailed", message = err.to_string());
            return Ok(());
        }
        let Ok(mut known) = self.families.lock() else {
            return Ok(());
        };

        if !self.collector.disable_target_info && !metrics.resource().is_empty() {
            let labels = to_pairs(get_attrs(&mut metrics.resource().iter(), &[]));
            encoder
                .encode_descriptor(
                    TARGET_INFO_NAME,
                    TARGET_INFO_DESCRIPTION,
                    None,
                    ClientMetricType::Gauge,
                )?
                .encode_family(&labels)?
                .encode_gauge(&1.0)?;
        }

        // Metrics with the same name from different scopes go in one family.
        let mut families: Vec<Family<'_>> = Vec::new();
        for scope_metrics in metrics.scope_metrics() {
            let mut scope_labels = Vec::new();
            if self.collector.scope_info_enabled {
                scope_labels = get_scope_labels(scope_metrics.scope());
                scope_labels.extend(self.collector.resource_selector.select(metrics.resource()));
            }
            for m in scope_metrics.metrics() {
                let Some((metric_type, name)) = self.metric_type_and_name(m) else {
                    continue;
                };
                let (drop, help) =
                    validate_metrics(&name, m.description(), metric_type, &mut known);
                if drop {
                    continue;
                }
                match families.iter_mut().find(|f| f.0 == name) {
                    Some(family) => family.3.push((m, scope_labels.clone())),
                    None => families.push((
                        name,
                        help.unwrap_or_else(|| m.description().to_string()),
                        metric_type,
                        vec![(m, scope_labels.clone())],
                    )),
                }
            }
        }

        for (name, help, metric_type, members) in families {
            let (name, metric_type) = match metric_type {
                // `prometheus_client` adds the `_total` suffix itself.
                MetricType::COUNTER => (
                    name.trim_end_matches(COUNTER_SUFFIX),
                    ClientMetricType::Counter,
                ),
                MetricType::HISTOGRAM => (name.as_str(), ClientMetricType::Histogram),
                _ => (name.as_str(), ClientMetricType::Gauge),
            };
            let mut family = encoder.encode_descriptor(name, &help, None, metric_type)?;
            for (m, extra) in members {
                match m.data() {
                    data::AggregatedMetrics::F64(d) => encode_data(&mut family, d, &extra)?,
                    data::AggregatedMetrics::I64(d) => encode_data(&mut family, d, &extra)?,
                    data::AggregatedMetrics::U64(d) => encode_data(&mut family, d, &extra)?,
                }
            }
        }

        Ok(())
    }
}

fn encode_data<T: Numeric + Copy>(
    family: &mut MetricEncoder<'_>,
    metric: &data::MetricData<T>,
    extra: &[LabelPair],
) -> Result<(), fmt::Error> {
    let labels = |attrs: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
        to_pairs(get_attrs(&mut attrs.map(|kv| (&kv.key, &kv.value)), extra))
    };

    match metric {
        data::MetricData::Sum(sum) => {
            for dp in sum.data_points() {
                let point_labels = labels(&mut dp.attributes());
                let mut point = family.encode_family(&point_labels)?;
                if sum.is_monotonic() {
                    point.encode_counter::<NoLabelSet, _, f64>(&dp.value().as_f64(), None)?;
                } else {
                    point.encode_gauge(&dp.value().as_f64())?;
                }
            }
        }
        data::MetricData::Gauge(gauge) => {
            for dp in gauge.data_points() {
                family
                    .encode_family(&labels(&mut dp.attributes()))?
                    .encode_gauge(&dp.value().as_f64())?;
            }
        }
        data::MetricData::Histogram(histogram) => {
            for dp in histogram.data_points() {
                let buckets: Vec<_> = dp.bounds().chain([INF]).zip(dp.bucket_counts()).collect();
                family
                    .encode_family(&labels(&mut dp.attributes()))?
                    .encode_histogram::<NoLabelSet>(
                        dp.sum().as_f64(),
                        dp.count(),
                        &buckets,
                        None,
                    )?;
            }
        }
        data::MetricData::ExponentialHistogram(histogram) => {
            for dp in histogram.data_points() {
                let (pos, neg) = (dp.positive_bucket(), dp.negative_bucket());
                let (mut pos_spans, pos_deltas) = to_spans(pos.offset(), pos.counts());
                let (neg_spans, neg_deltas) = to_spans(neg.offset(), neg.counts());
                // Prometheus reads a histogram with no spans, zero count and
                // zero threshold as a classic histogram.
                if pos_spans.is_empty() && neg_spans.is_empty() {
                    pos_spans.push((0, 0));
                }
                let native = NativeHistogram {
                    schema: dp.scale().into(),
                    zero_threshold: dp.zero_threshold(),
                    zero_count: dp.zero_count(),
                    positive: NativeHistogramBuckets {
                        spans: &pos_spans,
                        deltas: &pos_deltas,
                    },
                    negative: NativeHistogramBuckets {
                        spans: &neg_spans,
                        deltas: &neg_deltas,
                    },
                    created: Some(histogram.start_time()),
                };
                let count = dp.count() as u64;
                // The text format has no native histograms, so text shows only
                // the count and the sum.
                family
                    .encode_family(&labels(&mut dp.attributes()))?
                    .encode_histogram_with_native::<NoLabelSet>(
                        dp.sum().as_f64(),
                        count,
                        &[(INF, count)],
                        None,
                        native,
                    )?;
            }
        }
    }
    Ok(())
}

/// Converts dense OpenTelemetry bucket counts to Prometheus spans and deltas.
///
/// OpenTelemetry bucket `i` holds `(base^i, base^(i+1)]` and Prometheus bucket
/// `i` holds `(base^(i-1), base^i]`, so the Prometheus index is one more.
fn to_spans(offset: i32, counts: impl Iterator<Item = u64>) -> (Vec<(i32, u32)>, Vec<i64>) {
    let (mut spans, mut deltas) = (Vec::<(i32, u32)>::new(), Vec::new());
    let (mut previous, mut next_index) = (0i64, None);
    for (i, count) in counts.enumerate().filter(|(_, count)| *count > 0) {
        let index = offset + 1 + i as i32;
        match (next_index, spans.last_mut()) {
            (Some(next), Some(span)) if next == index => span.1 += 1,
            (Some(next), _) => spans.push((index - next, 1)),
            (None, _) => spans.push((index, 1)),
        }
        next_index = Some(index + 1);
        deltas.push(count as i64 - previous);
        previous = count as i64;
    }
    (spans, deltas)
}

fn to_pairs(labels: Vec<LabelPair>) -> Vec<(String, String)> {
    labels
        .into_iter()
        .map(|l| (l.name().to_string(), l.value().to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn to_spans_shifts_index_and_splits_on_empty_buckets() {
        let (spans, deltas) = super::to_spans(-68, [1, 0, 0, 1, 2, 0, 3].into_iter());
        assert_eq!(spans, vec![(-67, 1), (2, 2), (1, 1)]);
        assert_eq!(deltas, vec![1, 0, 1, 1]);
    }
}
