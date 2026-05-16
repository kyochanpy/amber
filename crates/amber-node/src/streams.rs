use std::collections::HashMap;

use amber_core::AmberConfig;
use dora_node_api::dora_core::config::{InputMapping, NodeRunConfig};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfiguredStream {
    pub(crate) node_id: String,
    pub(crate) output_id: String,
    pub(crate) every_n_frames: Option<u64>,
}

impl ConfiguredStream {
    pub(crate) fn new(
        node_id: impl Into<String>,
        output_id: impl Into<String>,
        every_n_frames: Option<u64>,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            output_id: output_id.into(),
            every_n_frames,
        }
    }

    pub(crate) fn schema_key(&self) -> StreamSchemaKey {
        StreamSchemaKey {
            node_id: self.node_id.clone(),
            output_id: self.output_id.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct StreamSchemaKey {
    pub(crate) node_id: String,
    pub(crate) output_id: String,
}

pub(crate) fn should_record_frame(
    frame_counters: &mut HashMap<StreamSchemaKey, u64>,
    stream: &ConfiguredStream,
) -> bool {
    let Some(every_n_frames) = stream.every_n_frames else {
        return true;
    };
    if every_n_frames <= 1 {
        return true;
    }

    let frame_count = frame_counters.entry(stream.schema_key()).or_insert(0);
    *frame_count += 1;
    (*frame_count).is_multiple_of(every_n_frames)
}

pub(crate) fn build_selected_inputs(
    config: &AmberConfig,
    node_config: &NodeRunConfig,
) -> HashMap<String, ConfiguredStream> {
    let selected_outputs = config
        .nodes
        .iter()
        .map(|node| {
            (
                node.id.clone(),
                node.outputs
                    .iter()
                    .map(|output| (output.id.clone(), output.every_n_frames))
                    .collect::<HashMap<_, _>>(),
            )
        })
        .collect::<HashMap<_, _>>();

    node_config
        .inputs
        .iter()
        .filter_map(|(input_id, input)| {
            let InputMapping::User(mapping) = &input.mapping else {
                return None;
            };

            let node_id = mapping.source.to_string();
            let output_id = mapping.output.to_string();
            let outputs = selected_outputs.get(&node_id)?;
            outputs.get(&output_id).map(|every_n_frames| {
                (
                    input_id.to_string(),
                    ConfiguredStream::new(node_id, output_id, *every_n_frames),
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use amber_core::AmberConfig;
    use dora_node_api::dora_core::config::{
        DataId, Input, InputMapping, NodeRunConfig, UserInputMapping,
    };

    use super::{ConfiguredStream, build_selected_inputs};

    #[test]
    fn build_selected_inputs_filters_unconfigured_streams() {
        let config = AmberConfig {
            nodes: vec![amber_core::NodeConfig {
                id: "camera".to_owned(),
                outputs: vec![amber_core::OutputConfig {
                    id: "image".to_owned(),
                    every_n_frames: None,
                }],
            }],
            ..AmberConfig::default()
        };
        let node_config = NodeRunConfig {
            inputs: BTreeMap::from([
                (
                    DataId::from("camera_image".to_owned()),
                    Input {
                        mapping: InputMapping::User(UserInputMapping {
                            source: "camera".to_owned().into(),
                            output: "image".to_owned().into(),
                        }),
                        queue_size: None,
                    },
                ),
                (
                    DataId::from("camera_depth".to_owned()),
                    Input {
                        mapping: InputMapping::User(UserInputMapping {
                            source: "camera".to_owned().into(),
                            output: "depth".to_owned().into(),
                        }),
                        queue_size: None,
                    },
                ),
            ]),
            outputs: BTreeSet::new(),
        };

        let selected = build_selected_inputs(&config, &node_config);

        assert_eq!(
            selected,
            HashMap::from([(
                "camera_image".to_owned(),
                ConfiguredStream::new("camera", "image", None),
            )])
        );
    }

    #[test]
    fn build_selected_inputs_preserves_every_n_frames() {
        let config = AmberConfig {
            nodes: vec![amber_core::NodeConfig {
                id: "camera".to_owned(),
                outputs: vec![amber_core::OutputConfig {
                    id: "image".to_owned(),
                    every_n_frames: Some(5),
                }],
            }],
            ..AmberConfig::default()
        };
        let node_config = NodeRunConfig {
            inputs: BTreeMap::from([(
                DataId::from("camera_image".to_owned()),
                Input {
                    mapping: InputMapping::User(UserInputMapping {
                        source: "camera".to_owned().into(),
                        output: "image".to_owned().into(),
                    }),
                    queue_size: None,
                },
            )]),
            outputs: BTreeSet::new(),
        };

        let selected = build_selected_inputs(&config, &node_config);

        assert_eq!(
            selected.get("camera_image"),
            Some(&ConfiguredStream::new("camera", "image", Some(5)))
        );
    }
}
