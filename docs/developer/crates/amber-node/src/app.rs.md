## root

### run()

```rust
pub async fn run() -> Result<()> {
    // dora node の起動
    let (node, mut events) = DoraNode::init_from_env()
        .map_err(|error| anyhow!("failed to initialize Dora node from environment: {error}"))?;
    // Amberのruntime起動
    let mut runtime = NodeRuntime::initialize_from_env().await?;
    // amberのruntimeにdora nodeのconfigを渡す
    // amberは
    runtime.configure_inputs(node.node_config());

    info!(
        session_id = %runtime.session_manifest.session_id,
        config_path = %runtime.config_path.display(),
        storage_backend = %runtime.config.storage.backend,
        selected_inputs = runtime.selected_inputs.len(),
        "amber-node startup completed"
    );

    let mut stop_requested = false;
    while let Some(event) = events.next().await {
        if runtime.handle_event(event).await? == EventHandling::StopRequested {
            stop_requested = true;
            break;
        }
    }

    if stop_requested {
        runtime.shutdown().await?;
    } else {
        warn!(
            session_id = %runtime.session_manifest.session_id,
            "Dora event stream ended without a stop event; skipping normal session close"
        );
    }

    Ok(())
}
```

## NodeRuntime

### initialize_from_env



### initializa_from_path


### configure_inputs