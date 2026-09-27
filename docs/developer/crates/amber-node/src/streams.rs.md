## ConfigureStream

## build_selected_inputs()

ノードが受け取る入力を変換する

```rust
pub(crate) fn build_selected_inputs(
    config: &AmberConfig,
    node_config: &NodeRunConfig,
) -> HashMap<String, ConfiguredStream> {
    // amber.ymlで指定されたノードたち
    // node_id -> (output_id -> every_n_frames) の二重HashMap
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

    // dataflow.ymlでamberに向けたノードたちをamber.ymlに書かれたものでフィルタする
    // dataflow.ymlで入れてるってことは収集する意思あるわけだから、引っかかった奴はフィードバックしてあげてもいいかも
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
```