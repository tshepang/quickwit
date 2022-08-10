// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::env;
use std::time::Duration;

use anyhow::Context;
use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use quickwit_cli::cli::{build_cli, CliCommand};
use quickwit_cli::QW_JAEGER_ENABLED_ENV_KEY;
use quickwit_cluster::QuickwitService;
use quickwit_common::metrics::new_gauge;
use quickwit_common::runtimes::RuntimesConfiguration;
use quickwit_serve::build_quickwit_build_info;
use quickwit_telemetry::payload::TelemetryEvent;
use tikv_jemallocator::Jemalloc;
use tracing::{error, info, Level};
use tracing_subscriber::fmt::time::UtcTime;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const JEMALLOC_METRICS_POLLING_INTERVAL: Duration = Duration::from_secs(1);

fn setup_logging_and_tracing(level: Level) -> anyhow::Result<()> {
    #[cfg(feature = "tokio-console")]
    {
        use quickwit_cli::QW_TOKIO_CONSOLE_ENABLED_ENV_KEY;
        if std::env::var_os(QW_TOKIO_CONSOLE_ENABLED_ENV_KEY).is_some() {
            console_subscriber::init();
            return Ok(());
        }
    }
    let env_filter = env::var("RUST_LOG")
        .map(|_| EnvFilter::from_default_env())
        .or_else(|_| EnvFilter::try_new(format!("quickwit={}", level)))
        .context("Failed to set up tracing env filter.")?;
    global::set_text_map_propagator(TraceContextPropagator::new());
    let registry = tracing_subscriber::registry().with(env_filter);
    let event_format = tracing_subscriber::fmt::format()
        .with_target(true)
        .with_timer(
            // We do not rely on the Rfc3339 implementation, because it has a nanosecond precision.
            // See discussion here: https://github.com/time-rs/time/discussions/418
            UtcTime::new(
                time::format_description::parse(
                    "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z",
                )
                .expect("Time format invalid."),
            ),
        );
    if std::env::var_os(QW_JAEGER_ENABLED_ENV_KEY).is_some() {
        // TODO: use install_batch once this issue is fixed: https://github.com/open-telemetry/opentelemetry-rust/issues/545
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name("quickwit")
            //.install_batch(opentelemetry::runtime::Tokio)
            .install_simple()
            .context("Failed to initialize Jaeger exporter.")?;
        registry
            .with(tracing_subscriber::fmt::layer().event_format(event_format))
            .with(tracing_opentelemetry::layer().with_tracer(tracer))
            .try_init()
            .context("Failed to set up tracing.")?
    } else {
        registry
            .with(tracing_subscriber::fmt::layer().event_format(event_format))
            .try_init()
            .context("Failed to set up tracing.")?
    }
    Ok(())
}

async fn jemalloc_metrics_loop() -> tikv_jemalloc_ctl::Result<()> {
    let allocated_gauge = new_gauge(
        "allocated_num_bytes",
        "Number of bytes allocated memory, as reported by jemallocated.",
        "quickwit",
    );

    // Obtain a MIB for the `epoch`, `stats.allocated`, and
    // `atats.resident` keys:
    let epoch_management_information_base = tikv_jemalloc_ctl::epoch::mib()?;
    let allocated = tikv_jemalloc_ctl::stats::allocated::mib()?;

    let mut poll_interval = tokio::time::interval(JEMALLOC_METRICS_POLLING_INTERVAL);

    loop {
        poll_interval.tick().await;

        // Many statistics are cached and only updated
        // when the epoch is advanced:
        epoch_management_information_base.advance()?;

        // Read statistics using MIB key:
        let allocated = allocated.read()?;

        allocated_gauge.set(allocated as i64);
    }
}

/// If a bunch of tokio runtimes need to be started for actors,
/// return the right configuration.
///
/// TODO making it configurable could be useful in the future.
fn runtime_configuration_for_cmd(command: &CliCommand) -> Option<RuntimesConfiguration> {
    match command {
        CliCommand::Run(run_cli_command) => {
            if run_cli_command.services.contains(&QuickwitService::Indexer) {
                Some(RuntimesConfiguration::default())
            } else {
                None
            }
        }
        CliCommand::Index(_) => Some(RuntimesConfiguration::default()),
        CliCommand::Split(_) | CliCommand::Source(_) => None,
    }
}

fn start_actor_runtimes(cli_command: &CliCommand) -> anyhow::Result<()> {
    if let Some(runtime_configuration) = runtime_configuration_for_cmd(cli_command) {
        quickwit_common::runtimes::initialize_runtimes(runtime_configuration)
            .context("Failed to start runtimes.")?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "openssl-support")]
    openssl_probe::init_ssl_cert_env_vars();

    let telemetry_handle = quickwit_telemetry::start_telemetry_loop();
    let about_text = about_text();
    let build_info = build_quickwit_build_info();

    let app = build_cli()
        .about(about_text.as_str())
        .version(build_info.version);
    let matches = app.get_matches();

    let command = match CliCommand::parse_cli_args(&matches) {
        Ok(command) => command,
        Err(err) => {
            eprintln!("Failed to parse command arguments: {:?}", err);
            std::process::exit(1);
        }
    };

    start_actor_runtimes(&command)?;

    tokio::task::spawn(async {
        if let Err(jemalloc_metrics_err) = jemalloc_metrics_loop().await {
            error!(err=?jemalloc_metrics_err, "Failed to gather metrics from jemalloc.");
        }
    });

    setup_logging_and_tracing(command.default_log_level())?;
    info!(
        version = build_info.version,
        commit = build_info.commit_short_hash,
    );

    let return_code: i32 = if let Err(err) = command.execute().await {
        eprintln!("Command failed: {:?}", err);
        1
    } else {
        0
    };

    quickwit_telemetry::send_telemetry_event(TelemetryEvent::EndCommand { return_code }).await;

    telemetry_handle.terminate_telemetry().await;
    global::shutdown_tracer_provider();

    std::process::exit(return_code)
}

/// Return the about text with telemetry info.
fn about_text() -> String {
    let mut about_text = String::from(
        "Index your dataset on object storage & make it searchable from the command line.\n  Find more information at https://quickwit.io/docs\n\n",
    );
    if quickwit_telemetry::is_telemetry_enabled() {
        about_text += "Telemetry: enabled";
    }
    about_text
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use quickwit_cli::cli::{build_cli, CliCommand};
    use quickwit_cli::index::{
        CreateIndexArgs, DeleteIndexArgs, DescribeIndexArgs, GarbageCollectIndexArgs,
        IndexCliCommand, IngestDocsArgs, MergeOrDemuxArgs, SearchIndexArgs,
    };
    use quickwit_cli::split::{DescribeSplitArgs, ExtractSplitArgs, SplitCliCommand};
    use quickwit_common::uri::Uri;

    #[test]
    fn test_parse_create_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let _ = app
            .try_get_matches_from(vec!["new", "--index-uri", "file:///indexes/wikipedia"])
            .unwrap_err();

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "create",
            "--index-config",
            "index-conf.yaml",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_index_config_uri = Uri::try_new(&format!(
            "file://{}/index-conf.yaml",
            std::env::current_dir().unwrap().display()
        ))
        .unwrap();
        let expected_cmd = CliCommand::Index(IndexCliCommand::Create(CreateIndexArgs {
            config_uri: Uri::try_new("file:///config.yaml").unwrap(),
            index_config_uri: expected_index_config_uri.clone(),
            overwrite: false,
            data_dir: None,
        }));
        assert_eq!(command, expected_cmd);

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "create",
            "--index-config",
            "index-conf.yaml",
            "--config",
            "/config.yaml",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_cmd = CliCommand::Index(IndexCliCommand::Create(CreateIndexArgs {
            config_uri: Uri::try_new("file:///config.yaml").unwrap(),
            index_config_uri: expected_index_config_uri,
            overwrite: true,
            data_dir: None,
        }));
        assert_eq!(command, expected_cmd);

        Ok(())
    }

    #[test]
    fn test_parse_ingest_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "ingest",
            "--index",
            "wikipedia",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    config_uri,
                    index_id,
                    input_path_opt: None,
                    overwrite: false,
                    data_dir: None,
                    clear_cache: true,
                })) if &index_id == "wikipedia"
                       && config_uri == Uri::try_new("file:///config.yaml").unwrap()
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "ingest",
            "--index",
            "wikipedia",
            "--config",
            "/config.yaml",
            "--keep-cache",
            "--overwrite",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Ingest(
                IngestDocsArgs {
                    config_uri,
                    index_id,
                    input_path_opt: None,
                    overwrite: true,
                    data_dir: None,
                    clear_cache: false
                })) if &index_id == "wikipedia"
                        && config_uri == Uri::try_new("file:///config.yaml").unwrap()
        ));
        Ok(())
    }

    #[test]
    fn test_parse_search_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "search",
            "--index",
            "wikipedia",
            "--query",
            "Barack Obama",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Search(SearchIndexArgs {
                index_id,
                query,
                max_hits: 20,
                start_offset: 0,
                search_fields: None,
                snippet_fields: None,
                start_timestamp: None,
                end_timestamp: None,
                aggregation: None,
                ..
            })) if &index_id == "wikipedia" && &query == "Barack Obama"
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "search",
            "--index",
            "wikipedia",
            "--query",
            "Barack Obama",
            "--max-hits",
            "50",
            "--start-offset",
            "100",
            "--start-timestamp",
            "0",
            "--end-timestamp",
            "1",
            "--search-fields",
            "title",
            "--snippet-fields",
            "body",
            "url",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let _config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Search(SearchIndexArgs {
                index_id,
                query,
                aggregation: None,
                max_hits: 50,
                start_offset: 100,
                search_fields: Some(search_field_names),
                snippet_fields: Some(snippet_field_names),
                start_timestamp: Some(0),
                end_timestamp: Some(1),
                config_uri: _config_uri,
                data_dir: None,
            })) if &index_id == "wikipedia"
                  && query == "Barack Obama"
                  && search_field_names == vec!["title".to_string(), "url".to_string()]
                  && snippet_field_names == vec!["body".to_string()]
        ));
        Ok(())
    }

    #[test]
    fn test_parse_delete_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "delete",
            "--index",
            "wikipedia",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Delete(DeleteIndexArgs {
                index_id,
                dry_run: false,
                ..
            })) if &index_id == "wikipedia"
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "delete",
            "--index",
            "wikipedia",
            "--dry-run",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Delete(DeleteIndexArgs {
                index_id,
                dry_run: true,
                ..
            })) if &index_id == "wikipedia"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_garbage_collect_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "gc",
            "--index",
            "wikipedia",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                dry_run: false,
                ..
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(60 * 60)
        ));

        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "gc",
            "--index",
            "wikipedia",
            "--grace-period",
            "5m",
            "--config",
            "/config.yaml",
            "--dry-run",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        let expected_config_uri = Uri::try_new("file:///config.yaml").unwrap();
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::GarbageCollect(GarbageCollectIndexArgs {
                index_id,
                grace_period,
                config_uri,
                dry_run: true,
                data_dir: None,
            })) if &index_id == "wikipedia" && grace_period == Duration::from_secs(5 * 60) && config_uri == expected_config_uri
        ));
        Ok(())
    }

    #[test]
    fn test_parse_merge_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "merge",
            "--index",
            "wikipedia",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Merge(MergeOrDemuxArgs {
                index_id,
                ..
            })) if &index_id == "wikipedia"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_demux_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "demux",
            "--index",
            "wikipedia",
            "--config",
            "quickwit.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Demux(MergeOrDemuxArgs {
                index_id,
                ..
            })) if &index_id == "wikipedia"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_describe_index_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "index",
            "describe",
            "--index",
            "wikipedia",
            "--config",
            "quickwit.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Index(IndexCliCommand::Describe(DescribeIndexArgs {
                index_id,
                ..
            })) if &index_id == "wikipedia"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_describe_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "split",
            "describe",
            "--index",
            "wikipedia",
            "--split",
            "ABC",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::Describe(DescribeSplitArgs {
                index_id,
                split_id,
                verbose: false,
                ..
            })) if &index_id == "wikipedia" && &split_id == "ABC"
        ));
        Ok(())
    }

    #[test]
    fn test_parse_split_extract_args() -> anyhow::Result<()> {
        let app = build_cli().no_binary_name(true);
        let matches = app.try_get_matches_from(vec![
            "split",
            "extract",
            "--index",
            "wikipedia",
            "--split",
            "ABC",
            "--target-dir",
            "datadir",
            "--config",
            "/config.yaml",
        ])?;
        let command = CliCommand::parse_cli_args(&matches)?;
        assert!(matches!(
            command,
            CliCommand::Split(SplitCliCommand::Extract(ExtractSplitArgs {
                index_id,
                split_id,
                target_dir,
                ..
            })) if &index_id == "wikipedia" && &split_id == "ABC" && target_dir == PathBuf::from("datadir")
        ));
        Ok(())
    }
}
