//! Integration test: driving the `tarantool/queue` module over iproto.
//!
//! This proves tarant can act as a full queue *consumer* and *producer*
//! against a real [`tarantool/queue`](https://github.com/tarantool/queue):
//! every task operation a remote client performs is a stored-function
//! call whose name embeds the tube — `queue.tube.<name>:take` and friends —
//! sent as `IPROTO_FUNCTION_NAME` verbatim, exactly as `net.box` sends it.
//!
//! It runs only when `TARANT_TEST_ADDR` points at an instance *and* that
//! instance can `require('queue')`. If the rock is not installed the test
//! skips cleanly, so CI without the module stays green.
//!
//! ```sh
//! # inside the container: tt rocks install queue  (or copy the pure-Lua module)
//! TARANT_TEST_ADDR=tarantool://tarant:tarant@127.0.0.1:3301 \
//!   cargo test --all-features --test queue
//! ```
//!
//! ## Two facts worth knowing
//!
//! * **`create_tube` needs `eval`, not `call`.** It returns the live tube
//!   object, which contains userdata/functions the iproto encoder cannot
//!   serialise (`unsupported Lua type 'userdata'`). Every *task* operation
//!   returns a plain tuple and works over `call`; only the constructor is
//!   `eval`-only. `on_task_change` is likewise `eval`-only — its argument is
//!   a Lua function.
//! * **A reconnect releases taken tasks.** The queue pins a taken task to the
//!   `box.session.id` that took it and releases it in the `_on_consumer_
//!   disconnect` trigger. tarant's reconnect opens a *new* session, so a task
//!   taken before a drop is back in `ready` afterwards and its `ack` fails
//!   with "Task was not taken". That is queue semantics, not a tarant bug.

use std::collections::BTreeMap;

use tarant::{Client, Value};

/// A normalised queue task as it crosses the wire: `[id, state, data]`.
type Task = (u64, String, String);

/// The tube these tests operate on. Alphanumeric, <= 32 chars, as the
/// module requires.
const TUBE: &str = "tarantqueuetest";

/// Connect, or return `None` when no server is configured.
async fn client() -> Option<Client> {
    let url = std::env::var("TARANT_TEST_ADDR").ok()?;
    Some(Client::connect(&url).await.expect("connect to TARANT_TEST_ADDR"))
}

/// Load the queue module into a global `queue`, returning `false` (so the
/// caller skips) when the rock is not installed.
async fn load_queue(client: &Client) -> bool {
    // `require` raises if the module is missing; catch it so a bare instance
    // skips instead of failing. On success we expose the usual `queue` global.
    let script = "local ok, mod = pcall(require, 'queue')
        if ok then queue = mod end
        return ok";
    let (ok,): (bool,) = client.eval(script, ()).await.expect("eval require('queue')");
    ok
}

/// A `MessagePack` map argument, e.g. `{ttl = 60}` — one call argument that is
/// a table, which is what the queue's option bags are.
fn opts(pairs: &[(&str, Value)]) -> Value {
    Value::Map(pairs.iter().map(|(k, v)| ((*k).into(), v.clone())).collect())
}

/// `tube:put(data, opts)` — returns the created task.
async fn put(client: &Client, data: &str, options: Value) -> Task {
    let (task,): (Task,) =
        client.call(&format!("queue.tube.{TUBE}:put"), (data, options)).await.expect("put");
    task
}

/// `tube:take(timeout)` — a task, or `None` when nothing is ready in time.
///
/// An empty take returns nil, which arrives as an empty `IPROTO_DATA`; a hit
/// arrives as `[[id, state, data]]`. Decoding into `Vec<Option<Task>>`
/// swallows both an empty array and a `[null]`, so either shape yields `None`.
async fn take(client: &Client, timeout: f64) -> Option<Task> {
    let tasks: Vec<Option<Task>> =
        client.call(&format!("queue.tube.{TUBE}:take"), (timeout,)).await.expect("take");
    tasks.into_iter().flatten().next()
}

/// A one-argument `tube:<op>(task_id)` that returns the affected task.
async fn task_op(client: &Client, op: &str, id: u64) -> Task {
    let (task,): (Task,) = client.call(&format!("queue.tube.{TUBE}:{op}"), (id,)).await.expect(op);
    task
}

#[tokio::test]
async fn fifottl_lifecycle() {
    let Some(client) = client().await else {
        eprintln!("skipping: TARANT_TEST_ADDR is not set");
        return;
    };
    if !load_queue(&client).await {
        eprintln!("skipping: this instance cannot require('queue')");
        return;
    }

    // Start from a clean slate, then create the tube. `create_tube` returns a
    // live object with userdata, so it must go through `eval`, not `call`.
    let reset = "local name = ...
        if queue.tube[name] ~= nil then queue.tube[name]:drop() end
        return true";
    let (_dropped,): (bool,) = client.eval(reset, (TUBE,)).await.expect("reset tube");
    let create = "local name = ...
        queue.create_tube(name, 'fifottl', {temporary = true, if_not_exists = true})
        return true";
    let (_created,): (bool,) = client.eval(create, (TUBE,)).await.expect("create_tube");

    // The queue is serving requests.
    let (state,): (String,) = client.call("queue.state", ()).await.expect("queue.state");
    assert_eq!(state, "RUNNING", "queue should be RUNNING on a rw instance");

    // put with a map of options, then take it and inspect [id, state, data].
    let (put_id, put_state, put_data) =
        put(&client, "hello", opts(&[("ttl", Value::from(60)), ("pri", Value::from(1))])).await;
    assert_eq!(put_state, "r", "a freshly put task is ready");
    assert_eq!(put_data, "hello");

    let (take_id, take_state, take_data) = take(&client, 1.0).await.expect("take a ready task");
    assert_eq!(take_id, put_id, "take returns the task we put");
    assert_eq!(take_state, "t", "a taken task is in the 'taken' state");
    assert_eq!(take_data, "hello");

    // ack the taken task: it moves to the acknowledged '-' state.
    let (_, ack_state, _) = task_op(&client, "ack", take_id).await;
    assert_eq!(ack_state, "-", "ack moves the task to the done state");

    // put + take + release: the task returns to ready and can be taken again.
    let (rel_put_id, ..) = put(&client, "release-me", opts(&[])).await;
    let (rel_take_id, ..) = take(&client, 1.0).await.expect("take before release");
    assert_eq!(rel_take_id, rel_put_id);
    let (_, rel_state, _) = task_op(&client, "release", rel_take_id).await;
    assert_eq!(rel_state, "r", "release puts the task back to ready");
    let (again_id, again_state, _) = take(&client, 1.0).await.expect("re-take a released task");
    assert_eq!(again_id, rel_put_id, "a released task can be taken again");
    assert_eq!(again_state, "t");
    task_op(&client, "ack", again_id).await; // tidy up so it is not left taken

    // put + take + bury + kick: a buried task is dug back out by kick.
    let (bury_put_id, ..) = put(&client, "bury-me", opts(&[])).await;
    let (bury_take_id, ..) = take(&client, 1.0).await.expect("take before bury");
    assert_eq!(bury_take_id, bury_put_id);
    let (_, bury_state, _) = task_op(&client, "bury", bury_take_id).await;
    assert_eq!(bury_state, "!", "bury moves the task to the buried state");
    let (kicked,): (u64,) =
        client.call(&format!("queue.tube.{TUBE}:kick"), (10u64,)).await.expect("kick");
    assert_eq!(kicked, 1, "kick digs out the one buried task");

    // delete removes a task outright, whatever its state.
    let (del_id, ..) = put(&client, "delete-me", opts(&[])).await;
    let (deleted_id, ..) = task_op(&client, "delete", del_id).await;
    assert_eq!(deleted_id, del_id, "delete returns the removed task");

    // statistics returns a map keyed by 'tasks' and 'calls'.
    let (tube_stats,): (BTreeMap<String, Value>,) =
        client.call("queue.statistics", (TUBE,)).await.expect("statistics");
    assert!(tube_stats.contains_key("tasks"), "statistics reports task counts");
    assert!(tube_stats.contains_key("calls"), "statistics reports call counts");

    // drop the tube: a plain call, returning nothing.
    let _dropped: Vec<Value> =
        client.call(&format!("queue.tube.{TUBE}:drop"), ()).await.expect("drop");
}
