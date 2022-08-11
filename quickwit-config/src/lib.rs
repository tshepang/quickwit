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

use anyhow::bail;
use once_cell::sync::OnceCell;
use regex::Regex;

mod config;
mod config_value;
mod index_config;
mod source_config;
mod templating;

pub use config::{
    get_searcher_config_instance, IndexerConfig, QuickwitConfig, SearcherConfig,
    DEFAULT_QW_CONFIG_PATH, SEARCHER_CONFIG_INSTANCE,
};
pub use index_config::{
    build_doc_mapper, DocMapping, IndexConfig, IndexingResources, IndexingSettings, MergePolicy,
    SearchSettings,
};
pub use source_config::{
    FileSourceParams, IngestApiSourceParams, KafkaSourceParams, KinesisSourceParams,
    RegionOrEndpoint, SourceConfig, SourceParams, VecSourceParams, VoidSourceParams,
    CLI_INGEST_SOURCE_ID,
};

fn is_false(val: &bool) -> bool {
    !*val
}

fn validate_identifier(label: &str, value: &str) -> anyhow::Result<()> {
    static IDENTIFIER_REGEX: OnceCell<Regex> = OnceCell::new();

    if IDENTIFIER_REGEX
        .get_or_init(|| Regex::new(r"^[a-zA-Z][a-zA-Z0-9-_]{2,254}$").expect("Failed to compile regular expression. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."))
        .is_match(value)
    {
        return Ok(());
    }
    bail!("{} `{}` is invalid.", label, value);
}

#[cfg(test)]
mod tests {

    use crate::validate_identifier;

    #[test]
    fn test_validate_identifier() {
        validate_identifier("Cluster ID", "").unwrap_err();
        validate_identifier("Cluster ID", "-").unwrap_err();
        validate_identifier("Cluster ID", "_").unwrap_err();
        validate_identifier("Cluster ID", "f").unwrap_err();
        validate_identifier("Cluster ID", "fo").unwrap_err();
        validate_identifier("Cluster ID", "_fo").unwrap_err();
        validate_identifier("Cluster ID", "_foo").unwrap_err();
        validate_identifier("Cluster ID", "foo").unwrap();
        validate_identifier("Cluster ID", "f-_").unwrap();

        assert_eq!(
            validate_identifier("Cluster ID", "foo!")
                .unwrap_err()
                .to_string(),
            "Cluster ID `foo!` is invalid."
        );
    }
}
