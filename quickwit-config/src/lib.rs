// Copyright (C) 2021 Quickwit, Inc.
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

use std::collections::HashMap;
use std::str::FromStr;
use serde::de::Visitor;

use anyhow::bail;
use once_cell::sync::OnceCell;
use regex::Regex;

mod config;
mod index_config;
mod source_config;

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

/// Injects value overrides read from environment variables.
fn inject(
    env_vars: &HashMap<String, String>,
    root: serde_yaml::Value,
) -> anyhow::Result<serde_yaml::Value> {
    inject_inner(env_vars, root, &mut Vec::new())
}

fn inject_inner(
    env_vars: &HashMap<String, String>,
    root: serde_yaml::Value,
    path: &mut Vec<String>,
) -> anyhow::Result<serde_yaml::Value> {
    match root {
        serde_yaml::Value::Bool(current_bool) => {
            let overridden_bool = find_override(env_vars, &path)?.unwrap_or(current_bool);
            Ok(serde_yaml::Value::Bool(overridden_bool))
        }
        serde_yaml::Value::Mapping(mapping) => {
            let mut overridden_mapping = serde_yaml::Mapping::new();
            for (key, value) in mapping {
                let overridden_value = if let serde_yaml::Value::String(key_str) = key {
                    path.push(key_str);
                    let overridden_value = inject_inner(env_vars, value, path)?;
                    let key = serde_yaml::Value::String(
                        path.pop()
                            .expect("It should return the key that was just inserted."),
                    );
                    overridden_mapping.insert(key, overridden_value);
                } else {
                    overridden_mapping.insert(key, value);
                };
            }
            Ok(serde_yaml::Value::Mapping(overridden_mapping))
        }
        serde_yaml::Value::Number(current_number) if current_number.is_u64() => {
            let overridden_u64 = find_override::<u64>(env_vars, &path)?
                .map(serde_yaml::Number::from)
                .unwrap_or(current_number);
            Ok(serde_yaml::Value::Number(overridden_u64))
        }
        serde_yaml::Value::Number(current_number) if current_number.is_i64() => {
            let overridden_i64 = find_override::<i64>(env_vars, &path)?
                .map(serde_yaml::Number::from)
                .unwrap_or(current_number);
            Ok(serde_yaml::Value::Number(overridden_i64))
        }
        serde_yaml::Value::Number(current_number) if current_number.is_f64() => {
            let overridden_f64 = find_override::<f64>(env_vars, &path)?
                .map(serde_yaml::Number::from)
                .unwrap_or(current_number);
            Ok(serde_yaml::Value::Number(overridden_f64))
        }
        serde_yaml::Value::Null => Ok(serde_yaml::Value::Null),

        serde_yaml::Value::String(current_string) => {
            let overridden_string = find_override(env_vars, &path)?.unwrap_or(current_string);
            Ok(serde_yaml::Value::String(overridden_string))
        }
    }
}

fn find_override<T: FromStr>(
    env_vars: &HashMap<String, String>,
    path: &[String],
) -> Result<Option<T>, <T as FromStr>::Err> {
    let env_var_key = format!("QW_{}", path.join(".").to_ascii_uppercase());

    if let Some(env_var_value_str) = env_vars.get(&env_var_key) {
        let env_var_value_t = env_var_value_str.parse::<T>()?;
        return Ok(Some(env_var_value_t));
    }
    Ok(None)
}

pub struct EnvVarDeserializer<'de> {
    inner: serde_yaml::Deserializer<'de>,
    path: Vec<String>,
}

impl<'de> serde::Deserializer<'de> for EnvVarDeserializer<'de> {
    type Error = serde_yamal::Error;

    fn deserialize_map<V>(self, visitor: V) -> serde_yaml::Result<V::Value>
    where
        V: Visitor<'de>,
    {
        self.inner.deserialize_map(visitor)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{find_override, inject, validate_identifier};

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

    #[test]
    fn test_find_override() {
        let mut env_vars = HashMap::new();
        env_vars.insert("QW_MY_BOOL".to_string(), "true".to_string());
        env_vars.insert("QW_MY_STRING".to_string(), "string".to_string());

        assert_eq!(
            find_override::<bool>(&env_vars, &["my_bool".to_string()])
                .unwrap()
                .unwrap(),
            true
        );
        assert_eq!(
            find_override::<String>(&env_vars, &["my_string".to_string()])
                .unwrap()
                .unwrap(),
            "string"
        );
    }

    #[test]
    fn test_inject_overrides() {
        let config_yaml = r#"
            key_bool: true
            key_u64: 0
            key_i64: -1
            key_f64: 0.0
            key_str: foo
            nested:
                key: bar
        "#;
        let config = serde_yaml::from_str::<serde_yaml::Value>(config_yaml).unwrap();

        let env_vars = [
            ("QW_KEY_BOOL", "false"),
            ("QW_KEY_U64", "1"),
            ("QW_KEY_I64", "-2"),
            ("QW_KEY_F64", "0.0"),
        ]
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

        let expected_config_yaml = r#"
            version: 0
            key: overridden-foo
            nested:
                key: overridden-bar
        "#;
        let expected_config =
            serde_yaml::from_str::<serde_yaml::Value>(expected_config_yaml).unwrap();

        let overridden_config = inject(&env_vars, config).unwrap();
        assert_eq!(overridden_config, expected_config)
    }
}
