use std::{path::PathBuf, time::Duration};

use communication_layer_request_reply::TcpRequestReplyConnection;
use eyre::{bail, Context};
use uuid::Uuid;

use crate::{
    descriptor::Descriptor,
    topics::{ControlRequest, ControlRequestReply, DataflowId},
};

pub fn start_dataflow(
    dataflow: Descriptor,
    name: Option<String>,
    local_working_dir: PathBuf,
    session: &mut TcpRequestReplyConnection,
) -> Result<Uuid, eyre::ErrReport> {
    let reply_raw = session
        .request(
            &serde_json::to_vec(&ControlRequest::Start {
                dataflow,
                name,
                local_working_dir,
            })
            .unwrap(),
        )
        .wrap_err("failed to send start dataflow message")?;

    let result: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;
    match result {
        ControlRequestReply::DataflowStarted { uuid } => {
            eprintln!("{uuid}");
            Ok(uuid)
        }
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected start dataflow reply: {other:?}"),
    }
}

pub fn stop_dataflow(
    uuid: Uuid,
    grace_duration: Option<Duration>,
    session: &mut TcpRequestReplyConnection,
) -> Result<(), eyre::ErrReport> {
    let reply_raw = session
        .request(
            &serde_json::to_vec(&ControlRequest::Stop {
                dataflow_uuid: uuid,
                grace_duration,
            })
            .unwrap(),
        )
        .wrap_err("failed to send dataflow stop message")?;
    let result: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;
    match result {
        ControlRequestReply::DataflowStopped { uuid: _, result } => result
            .map_err(|err| eyre::eyre!(err))
            .wrap_err("dataflow failed"),
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected stop dataflow reply: {other:?}"),
    }
}

pub fn stop_dataflow_by_name(
    name: String,
    grace_duration: Option<Duration>,
    session: &mut TcpRequestReplyConnection,
) -> Result<(), eyre::ErrReport> {
    let reply_raw = session
        .request(
            &serde_json::to_vec(&ControlRequest::StopByName {
                name,
                grace_duration,
            })
            .unwrap(),
        )
        .wrap_err("failed to send dataflow stop_by_name message")?;
    let result: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;
    match result {
        ControlRequestReply::DataflowStopped { uuid: _, result } => result
            .map_err(|err| eyre::eyre!(err))
            .wrap_err("dataflow failed"),
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected stop dataflow reply: {other:?}"),
    }
}

pub fn list(session: &mut TcpRequestReplyConnection) -> Result<(), eyre::ErrReport> {
    let ids = query_running_dataflows(session)?;

    if ids.is_empty() {
        eprintln!("No dataflows are running");
    } else {
        println!("Running dataflows:");
        for id in ids {
            println!("- {id}");
        }
    }

    Ok(())
}

pub fn query_running_dataflows(
    session: &mut TcpRequestReplyConnection,
) -> Result<Vec<DataflowId>, eyre::ErrReport> {
    let reply_raw = session
        .request(&serde_json::to_vec(&ControlRequest::List).unwrap())
        .wrap_err("failed to send list message")?;
    let reply: ControlRequestReply =
        serde_json::from_slice(&reply_raw).wrap_err("failed to parse reply")?;
    let ids = match reply {
        ControlRequestReply::DataflowList { dataflows } => dataflows,
        ControlRequestReply::Error(err) => bail!("{err}"),
        other => bail!("unexpected list dataflow reply: {other:?}"),
    };

    Ok(ids)
}
