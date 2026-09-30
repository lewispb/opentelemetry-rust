#![cfg(feature = "prometheus-client")]

use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use prometheus_client::encoding::{prometheus_protobuf, text};
use prometheus_client::registry::Registry;

#[test]
fn exponential_histogram_is_exported_as_native_histogram() {
    let mut registry = Registry::default();
    let exporter = opentelemetry_prometheus::exporter()
        .without_target_info()
        .scope_info_enabled(false)
        .build_for_prometheus_client(&mut registry)
        .unwrap();
    let view = |_: &Instrument| {
        Stream::builder()
            .with_aggregation(Aggregation::Base2ExponentialHistogram {
                max_size: 160,
                max_scale: 8,
                record_min_max: true,
            })
            .build()
            .ok()
    };
    let provider = SdkMeterProvider::builder()
        .with_reader(exporter)
        .with_view(view)
        .build();

    let latency = provider.meter("test").f64_histogram("latency").build();
    for v in [0.0, 0.5, 1.5, -2.0] {
        latency.record(v, &[]);
    }

    let families = prometheus_protobuf::encode(&registry).unwrap();
    let h = families[0].metric[0].histogram.as_ref().unwrap();
    assert_eq!(families[0].name, "latency");
    assert_eq!((h.sample_count, h.sample_sum, h.zero_count), (4, 0.0, 1));
    assert_eq!(h.positive_delta.iter().sum::<i64>(), 1);
    assert_eq!(h.positive_span.len(), 2);
    assert_eq!(h.negative_span.len(), 1);

    let mut out = String::new();
    text::encode(&mut out, &registry).unwrap();
    assert!(out.contains("latency_count{} 4"), "{out}");
}
