//! Integration tests against a real Tarantool.
//!
//! These are the tests that prove the wire format is right: unit tests can
//! only check that we encode what we *think* the protocol is. Point
//! `TARANT_TEST_ADDR` at an instance whose user can create spaces —
//! `deploy/dev/tarantool.yaml` brings one up — and they run; without it they
//! skip, so `cargo test` stays green on a machine with no server.
//!
//! ```sh
//! kubectl apply -f deploy/dev/tarantool.yaml
//! export TARANT_TEST_ADDR="tarantool://tarant:tarant@$(
//!   kubectl -n tarant get svc tarantool -o jsonpath='{.spec.clusterIP}'):3301"
//! cargo test --test live
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tarant::{Client, ErrorCode, Isolation, Iter, TxOptions, Update, Value};

/// A row of the space each test creates for itself.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
    age: u32,
}

/// Connect, or return `None` when no server is configured.
async fn client() -> Option<Client> {
    let url = std::env::var("TARANT_TEST_ADDR").ok()?;
    Some(Client::connect(&url).await.expect("connect to TARANT_TEST_ADDR"))
}

/// Run `body` against a freshly created space, dropped afterwards.
///
/// Each test gets its own space name so they can run concurrently, which is
/// what `cargo test` does by default.
async fn with_space<F, Fut>(name: &str, body: F)
where
    F: FnOnce(Client, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let Some(client) = client().await else {
        eprintln!("skipping: TARANT_TEST_ADDR is not set");
        return;
    };
    let space = format!("tarant_test_{name}");
    let _: Vec<tarant::Value> = client
        .eval(
            "local name = ...
             if box.space[name] then box.space[name]:drop() end
             local s = box.schema.space.create(name)
             s:format({{name='id', type='unsigned'},
                       {name='name', type='string'},
                       {name='age', type='unsigned'}})
             s:create_index('primary', {parts={'id'}})
             s:create_index('age', {parts={'age'}, unique=false})
             return {}",
            (&space,),
        )
        .await
        .expect("create the test space");

    body(client.clone(), space.clone()).await;

    let _: Vec<tarant::Value> = client
        .eval(
            "local name = ... if box.space[name] then box.space[name]:drop() end return {}",
            (&space,),
        )
        .await
        .expect("drop the test space");
}

#[tokio::test]
async fn handshake_reports_a_modern_server() {
    let Some(client) = client().await else { return };
    let info = client.server_info();
    assert!(info.version().starts_with('3'), "expected a 3.x server, got {}", info.version());
    assert!(info.supports(tarant::Feature::Watchers));
    assert!(info.supports(tarant::Feature::Transactions));
    assert!(info.supports(tarant::Feature::SpaceAndIndexNames));
    client.ping().await.expect("ping");
}

#[tokio::test]
async fn crud_round_trips_a_typed_tuple() {
    with_space("crud", |client, space| async move {
        let users = client.space::<User>(&space);
        let ann = User { id: 1, name: "ann".into(), age: 30 };

        users.insert(&ann).await.expect("insert");
        assert_eq!(users.get(1u64).await.expect("get"), Some(ann.clone()));
        assert_eq!(users.get(2u64).await.expect("get missing"), None);

        // A second insert of the same key is a typed, matchable rejection.
        let err = users.insert(&ann).await.expect_err("duplicate insert must fail");
        assert_eq!(err.as_server().map(tarant::ServerError::code), Some(ErrorCode::TUPLE_FOUND));
        assert!(!err.is_transient());

        // Replace overwrites; update mutates one field.
        users.replace(&User { id: 1, name: "ann".into(), age: 31 }).await.expect("replace");
        let updated = users.update(1u64, Update::new().add(3, 1)).await.expect("update");
        assert_eq!(updated.map(|u| u.age), Some(32));

        let removed = users.delete(1u64).await.expect("delete");
        assert_eq!(removed.map(|u| u.name), Some("ann".to_owned()));
        assert_eq!(users.get(1u64).await.expect("get after delete"), None);
    })
    .await;
}

#[tokio::test]
async fn select_walks_a_secondary_index() {
    with_space("select", |client, space| async move {
        let users = client.space::<User>(&space);
        for (id, age) in [(1u64, 20u32), (2, 30), (3, 40), (4, 50)] {
            users.insert(&User { id, name: format!("u{id}"), age }).await.expect("insert");
        }

        let adults: Vec<User> =
            users.index("age").select(30u32).iterator(Iter::Ge).await.expect("select ge");
        assert_eq!(adults.iter().map(|u| u.age).collect::<Vec<_>>(), vec![30, 40, 50]);

        let all: Vec<User> = users.select(()).iterator(Iter::All).await.expect("select all");
        assert_eq!(all.len(), 4);

        let limited: Vec<User> =
            users.select(()).iterator(Iter::All).limit(2).await.expect("select limited");
        assert_eq!(limited.len(), 2);

        let skipped: Vec<User> =
            users.select(()).iterator(Iter::All).offset(3).await.expect("select offset");
        assert_eq!(skipped.len(), 1);
    })
    .await;
}

#[tokio::test]
async fn pagination_walks_the_whole_space_by_cursor() {
    with_space("paginate", |client, space| async move {
        let users = client.space::<User>(&space);
        for id in 1u64..=10 {
            users
                .insert(&User { id, name: format!("u{id}"), age: 20 + u32::try_from(id).unwrap() })
                .await
                .expect("insert");
        }

        let mut seen = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut select = users.select(()).iterator(Iter::All).limit(3);
            if let Some(position) = &cursor {
                select = select.after(position);
            }
            let page = select.page().await.expect("page");
            if page.rows.is_empty() {
                break;
            }
            seen.extend(page.rows.iter().map(|u| u.id));
            cursor = page.position;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(seen, (1u64..=10).collect::<Vec<_>>(), "every row exactly once, in order");
    })
    .await;
}

#[tokio::test]
async fn upsert_inserts_then_updates() {
    with_space("upsert", |client, space| async move {
        let users = client.space::<User>(&space);
        let seed = User { id: 7, name: "seed".into(), age: 1 };

        users.upsert(&seed, Update::new().add(3, 10)).await.expect("upsert insert");
        assert_eq!(users.get(7u64).await.expect("get").map(|u| u.age), Some(1));

        users.upsert(&seed, Update::new().add(3, 10)).await.expect("upsert update");
        assert_eq!(users.get(7u64).await.expect("get").map(|u| u.age), Some(11));
    })
    .await;
}

#[tokio::test]
async fn call_and_eval_carry_typed_arguments() {
    let Some(client) = client().await else { return };

    let (sum,): (i64,) = client.eval("local a, b = ... return a + b", (2, 3)).await.expect("eval");
    assert_eq!(sum, 5);

    let (greeting,): (String,) =
        client.eval("local who = ... return 'hello, ' .. who", ("world",)).await.expect("eval str");
    assert_eq!(greeting, "hello, world");

    let (min, max): (i64, i64) = client.eval("return 1, 9", ()).await.expect("eval multi");
    assert_eq!((min, max), (1, 9));

    // A Lua error comes back as a matchable server error, not a panic.
    let err = client
        .eval::<Vec<tarant::Value>>("error('boom')", ())
        .await
        .expect_err("a raised Lua error must surface");
    let server = err.as_server().expect("a server error");
    assert!(server.message.contains("boom"), "message was {:?}", server.message);
}

#[tokio::test]
async fn unknown_space_is_a_typed_error() {
    let Some(client) = client().await else { return };
    let missing = client.space::<User>("tarant_no_such_space");
    let err = missing.get(1u64).await.expect_err("a missing space must fail");
    assert_eq!(err.as_server().map(tarant::ServerError::code), Some(ErrorCode::NO_SUCH_SPACE));
}

#[tokio::test]
async fn transactions_commit_and_roll_back() {
    with_space("tx", |client, space| async move {
        let users = client.space::<User>(&space);
        let insert = format!("box.space.{space}:insert{{...}} return {{}}");

        // Rolled back: nothing must survive.
        let mut tx = client.stream();
        tx.begin(Isolation::ReadConfirmed).await.expect("begin");
        let _: Vec<Value> = tx.eval(&insert, (99u64, "rolled-back", 1u32)).await.expect("insert");
        tx.rollback().await.expect("rollback");
        assert_eq!(users.get(99u64).await.expect("get after rollback"), None);

        // Committed: it must be visible to a plain read afterwards.
        let mut tx = client.stream();
        tx.begin(TxOptions::new().timeout(Duration::from_secs(5))).await.expect("begin");
        let _: Vec<Value> = tx.eval(&insert, (100u64, "committed", 2u32)).await.expect("insert");
        tx.commit().await.expect("commit");
        assert_eq!(
            users.get(100u64).await.expect("get after commit").map(|u| u.name),
            Some("committed".to_owned())
        );

        // Dropping mid-transaction fires a rollback for us.
        {
            let mut tx = client.stream();
            tx.begin(Isolation::Default).await.expect("begin");
            let _: Vec<Value> =
                tx.eval(&insert, (101u64, "abandoned", 3u32)).await.expect("insert");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(users.get(101u64).await.expect("get after drop"), None);
    })
    .await;
}

#[tokio::test]
async fn watchers_receive_broadcasts() {
    let Some(client) = client().await else { return };
    let key = "tarant.test.key";

    let _: Vec<tarant::Value> = client
        .eval("box.broadcast('tarant.test.key', 'first') return {}", ())
        .await
        .expect("broadcast first");

    let mut watcher = client.watch(key).await.expect("watch");
    assert_eq!(watcher.get::<String>().expect("first value"), "first");

    let _: Vec<tarant::Value> = client
        .eval("box.broadcast('tarant.test.key', 'second') return {}", ())
        .await
        .expect("broadcast second");

    tokio::time::timeout(Duration::from_secs(5), watcher.changed())
        .await
        .expect("an update within 5s")
        .expect("watcher still live");
    assert_eq!(watcher.get::<String>().expect("second value"), "second");
}

#[tokio::test]
async fn concurrent_requests_share_one_connection() {
    let Some(client) = client().await else { return };
    // 64 requests in flight at once: if sync matching were wrong, replies
    // would land on the wrong caller and the sums would not match.
    let mut tasks = tokio::task::JoinSet::new();
    for n in 0i64..64 {
        let client = client.clone();
        tasks.spawn(async move {
            let (doubled,): (i64,) =
                client.eval("local n = ... return n * 2", (n,)).await.expect("eval");
            (n, doubled)
        });
    }
    while let Some(result) = tasks.join_next().await {
        let (n, doubled) = result.expect("task");
        assert_eq!(doubled, n * 2, "reply {doubled} landed on the wrong request {n}");
    }
}

#[tokio::test]
async fn bad_credentials_fail_at_connect() {
    let Some(url) = std::env::var("TARANT_TEST_ADDR").ok() else { return };
    let wrong = url.replace(":tarant@", ":definitely-wrong@");
    if wrong == url {
        return; // the configured URL has no password to break
    }
    let err = Client::connect(&wrong).await.expect_err("wrong password must fail");
    assert!(matches!(err, tarant::Error::Auth { .. }), "got {err:?}");
}
