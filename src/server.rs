use eyre::eyre;
use eyre::WrapErr;
use futures::future::join_all;
use futures::prelude::*;
use pyo3::prelude::*;
use serde::Deserialize;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;
use std::hash::Hasher;
use std::time::{Duration, Instant};
use tokio::time::timeout;
use zenoh::config::Config;
use zenoh::prelude::SplitBuffer;

use crate::python_binding::{call, init};

static DURATION_MILLIS: u64 = 1;
#[derive(Deserialize, Debug)]
struct ConfigVariables {
    subscriptions: Vec<String>,
}

#[pyo3_asyncio::tokio::main]
pub async fn main() -> PyResult<()> {
    // Subscribe
    let variables = envy::from_env::<ConfigVariables>().unwrap();

    env_logger::init();
    let config = Config::default();
    let session = zenoh::open(config).await.unwrap();

    // Create a hashmap of all subscriptions.
    let mut subscribers = HashMap::new();
    let subs = variables.subscriptions.clone();

    for subscription in &subs {
        subscribers.insert(subscription.clone(), session
            .subscribe(subscription)
            .await
            .map_err(|err| {
                eyre!("Could not subscribe to the given subscription key expression. Error: {err}")
            })
            .unwrap());
    }

    // Store the latest value of all subscription as well as the output of the function. hash the state to easily check if the state has changed.
    let mut states = BTreeMap::new();
    let mut states_hash = hash(&states);

    let py_function = init()
        .wrap_err("Failed to init the Python Function")
        .unwrap();
    let duration = Duration::from_millis(DURATION_MILLIS);
    let mut futures_put = vec![];

    loop {
        let now = Instant::now();
        let mut futures = vec![];
        for (_, v) in subscribers.iter_mut() {
            futures.push(timeout(duration, v.next()));
        }

        let results = join_all(futures).await;

        for (result, subscription) in results.into_iter().zip(&subs) {
            if let Ok(Some(data)) = result {
                let value = data.value.payload;
                let binary = value.contiguous();
                states.insert(
                    subscription.clone().to_string(),
                    String::from_utf8(binary.to_vec()).unwrap(),
                );
            }
        }

        let new_hash = hash(&states);

        if states_hash == new_hash {
            continue;
        }

        let now = Instant::now();
        let outputs = call(&py_function, states.clone()).await.unwrap();
        println!("call python {:#?}", now.elapsed());

        for (key, value) in outputs {
            states.insert(key.clone(), value.clone());
            futures_put.push(session.put(key, value));
        }

        states_hash = hash(&states);

        println!("loop {:#?}", now.elapsed());
    }
}

fn hash(states: &BTreeMap<String, String>) -> u64 {
    let mut hasher = DefaultHasher::new();
    states.hash(&mut hasher);
    hasher.finish()
}
