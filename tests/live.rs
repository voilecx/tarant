//! Integration tests against a real Tarantool.
//!
//! These are the tests that prove the wire format is right: unit tests can
//! only check that we encode what we *think* the protocol is. Point
//! `TARANT_TEST_ADDR` at an instance whose user can create spaces — the
//! `compose.yaml` at the repository root brings one up — and they run;
//! without it they skip, so `cargo test` stays green on a machine with no
//! server.
//!
//! ```sh
//! docker compose up --wait
//! TARANT_TEST_ADDR=tarantool://tarant:tarant@127.0.0.1:3301 cargo test --test live
//! ```

use std::time::Duration;

use serde::{Deserialize, Serialize};
use tarant::sql::Named;
use tarant::{
    Client, Datetime, Decimal, ErrorCode, Interval, Isolation, Iter, TxOptions, Update, Uuid, Value,
};
use tokio::sync::RwLock;

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
    ddl(&client, CREATE, &space).await.expect("create the test space");
    {
        let _schema_quiet = SCHEMA.read().await;
        body(client.clone(), space.clone()).await;
    }
    ddl(&client, DROP, &space).await.expect("drop the test space");
}

const CREATE: &str = "local name = ...
    if box.space[name] then box.space[name]:drop() end
    local s = box.schema.space.create(name)
    s:format({{name='id', type='unsigned'},
              {name='name', type='string'},
              {name='age', type='unsigned'}})
    s:create_index('primary', {parts={'id'}})
    s:create_index('age', {parts={'age'}, unique=false})
    return {}";

const DROP: &str = "local name = ... if box.space[name] then box.space[name]:drop() end return {}";

/// Keeps schema changes apart from running tests.
///
/// Under MVCC, Tarantool 3.0 aborts every open transaction when the schema
/// changes and rejects concurrent DDL with `TRANSACTION_CONFLICT`; later
/// releases queue it. So DDL takes the write side and a test body the read
/// side: bodies still run, and pipeline their requests, in parallel.
static SCHEMA: RwLock<()> = RwLock::const_new(());

async fn ddl(client: &Client, script: &str, space: &str) -> tarant::Result<()> {
    let _schema_exclusive = SCHEMA.write().await;
    client.eval::<Vec<Value>>(script, (space,)).await.map(drop)
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

/// A space whose fields are the four extension types.
const CREATE_TYPED: &str = "local name = ...
    if box.space[name] then box.space[name]:drop() end
    local s = box.schema.space.create(name)
    s:format({{name='id', type='uuid'},
              {name='price', type='decimal'},
              {name='at', type='datetime'},
              {name='span', type='interval'}})
    s:create_index('primary', {parts={'id'}})
    s:create_index('price', {parts={'price'}, unique=false})
    return {}";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TypedRow {
    id: Uuid,
    price: Decimal,
    at: Datetime,
    span: Interval,
}

#[tokio::test]
async fn extension_types_round_trip_through_a_space() {
    let Some(client) = client().await else { return };
    let space = "tarant_test_types".to_owned();
    ddl(&client, CREATE_TYPED, &space).await.expect("create the typed space");
    {
        let _schema_quiet = SCHEMA.read().await;

        let row = TypedRow {
            id: "f6423bdf-b49e-4913-b361-0740c9702e4b".parse().unwrap(),
            price: "19.99".parse().unwrap(),
            at: Datetime::from_unix(1_700_000_000, 123_456_789).with_tz_offset(180),
            span: Interval::months(1) + Interval::days(-3),
        };
        let rows = client.space::<TypedRow>(&space);
        rows.insert(&row).await.expect("insert typed row");

        let back = rows.get(row.id).await.expect("get by uuid").expect("the row");
        assert_eq!(back, row);
        assert_eq!(back.at.tz_offset_minutes(), 180, "the zone survives the round trip");
        assert_eq!(back.price.to_string(), "19.99");

        let pricey: Vec<TypedRow> = rows
            .index("price")
            .select(Decimal::from(10u32))
            .iterator(Iter::Ge)
            .await
            .expect("select by decimal");
        assert_eq!(pricey.len(), 1);
        let cheap: Vec<TypedRow> = rows
            .index("price")
            .select(Decimal::from(20u32))
            .iterator(Iter::Ge)
            .await
            .expect("select");
        assert!(cheap.is_empty());

        // The server agrees on what it received.
        let (price, at, span): (String, String, String) = client
            .eval(
                "local name, id = ...
                 local t = box.space[name]:get(id)
                 return tostring(t.price), tostring(t.at), tostring(t.span)",
                (&space, row.id),
            )
            .await
            .expect("eval");
        assert_eq!(price, "19.99");
        assert_eq!(at, "2023-11-15T01:13:20.123456789+0300");
        assert_eq!(span, "+1 months, -3 days");

        // And SQL sees the same values through the same types.
        let via_sql = client
            .query::<(Uuid, Decimal)>(
                &format!("SELECT \"id\", \"price\" FROM SEQSCAN \"{space}\""),
                (),
            )
            .await
            .expect("sql over a formatted space");
        assert_eq!(via_sql.rows, vec![(row.id, row.price.clone())]);
    }
    ddl(&client, DROP, &space).await.expect("drop the typed space");
}

#[tokio::test]
async fn sql_statements_query_execute_and_prepare() {
    let Some(client) = client().await else { return };
    let table = "TARANT_TEST_SQL";
    sql_ddl(&client, &format!("DROP TABLE IF EXISTS {table}")).await.expect("drop");
    sql_ddl(
        &client,
        &format!(
            "CREATE TABLE {table} (id INTEGER PRIMARY KEY AUTOINCREMENT, name STRING NOT NULL, age INTEGER)"
        ),
    )
    .await
    .expect("create table");
    {
        let _schema_quiet = SCHEMA.read().await;

        let done = client
            .execute(
                &format!("INSERT INTO {table} (name, age) VALUES (?, ?), (?, ?)"),
                ("ann", 30, "bob", 17),
            )
            .await
            .expect("insert");
        assert_eq!(done.row_count, 2);
        assert_eq!(done.autoincrement_ids, vec![1, 2]);

        let adults = client
            .query::<(u64, String, i64)>(
                &format!("SELECT id, name, age FROM SEQSCAN {table} WHERE age >= :min ORDER BY id"),
                (Named("min", 18),),
            )
            .await
            .expect("query");
        assert_eq!(adults.rows, vec![(1, "ann".to_owned(), 30)]);
        // 3.x folds unquoted identifiers to lower case, 2.x to upper.
        let names: Vec<String> = adults.columns.iter().map(|c| c.name.to_lowercase()).collect();
        assert_eq!(names, ["id", "name", "age"]);
        assert_eq!(adults.columns[1].type_name, "string");

        // Asking for rows from a statement that has none is a decode error, not a panic.
        let err = client
            .query::<Value>(&format!("DELETE FROM {table} WHERE id = 99"), ())
            .await
            .expect_err("no result set");
        assert!(matches!(err, tarant::Error::Decode(_)), "got {err:?}");

        let by_id = client
            .prepare(&format!("SELECT name FROM {table} WHERE id = ?"))
            .await
            .expect("prepare");
        assert_eq!(by_id.parameter_count(), 1);
        assert_eq!(by_id.columns()[0].name.to_lowercase(), "name");
        let bob = by_id.query::<(String,)>(2u64).await.expect("prepared query");
        assert_eq!(bob.rows, vec![("bob".to_owned(),)]);
        let rename = client
            .prepare(&format!("UPDATE {table} SET name = ? WHERE id = ?"))
            .await
            .expect("prepare");
        assert_eq!(rename.execute(("robert", 2u64)).await.expect("prepared execute").row_count, 1);
        rename.unprepare().await.expect("unprepare");

        // SQL inside a stream transaction rolls back with it.
        let mut tx = client.stream();
        tx.begin(Isolation::Default).await.expect("begin");
        tx.execute(&format!("INSERT INTO {table} (name, age) VALUES ('tmp', 1)"), ())
            .await
            .expect("tx insert");
        let inside = tx
            .query::<(u64,)>(&format!("SELECT COUNT(*) FROM SEQSCAN {table}"), ())
            .await
            .expect("tx count");
        assert_eq!(inside.rows[0].0, 3);
        let seen = tx.query_prepared::<(String,)>(&by_id, 2u64).await.expect("prepared in tx");
        assert_eq!(seen.rows[0].0, "robert");
        tx.rollback().await.expect("rollback");
        let after = client
            .query::<(u64,)>(&format!("SELECT COUNT(*) FROM SEQSCAN {table}"), ())
            .await
            .expect("count");
        assert_eq!(after.rows[0].0, 2);
    }
    sql_ddl(&client, &format!("DROP TABLE {table}")).await.expect("drop table");
}

/// Schema changes through SQL take the same lock as [`ddl`].
async fn sql_ddl(client: &Client, sql: &str) -> tarant::Result<()> {
    let _schema_exclusive = SCHEMA.write().await;
    client.execute(sql, ()).await.map(drop)
}

#[tokio::test]
async fn synchronous_transactions_commit_with_a_quorum_of_one() {
    let Some(client) = client().await else { return };
    if !client.server_info().supports(tarant::Feature::IsSync) {
        eprintln!("skipping: the server has no IS_SYNC");
        return;
    }
    // `box.ctl.promote()` changes instance-wide limbo state, so no other
    // transaction may run alongside it: take the schema lock exclusively.
    let _exclusive = SCHEMA.write().await;
    let space = "tarant_test_sync".to_owned();
    // Already holding the exclusive lock, so run the DDL directly rather than
    // through `ddl`, which would try to take the same lock again and deadlock.
    let _: Vec<Value> = client.eval(CREATE, (&space,)).await.expect("create");
    let _: Vec<Value> = client.eval("box.ctl.promote() return {}", ()).await.expect("promote");
    let users = client.space::<User>(&space);
    let insert = format!("box.space.{space}:insert{{...}} return {{}}");
    let mut tx = client.stream();
    tx.begin(TxOptions::new().synchronous()).await.expect("begin synchronous");
    let _: Vec<Value> = tx.eval(&insert, (1u64, "sync", 1u32)).await.expect("insert");
    tx.commit().await.expect("a single instance is its own quorum");
    assert!(users.get(1u64).await.expect("get").is_some());
    let _: Vec<Value> = client.eval("box.ctl.demote() return {}", ()).await.expect("demote");
    let _: Vec<Value> = client.eval(DROP, (&space,)).await.expect("drop");
}

#[tokio::test]
async fn pagination_resumes_after_a_tuple() {
    with_space("after_tuple", |client, space| async move {
        let users = client.space::<User>(&space);
        for id in 1u64..=5 {
            users.insert(&User { id, name: format!("u{id}"), age: 20 }).await.expect("insert");
        }
        let first: Vec<User> = users.select(()).iterator(Iter::All).limit(2).await.expect("first");
        let rest: Vec<User> =
            users.select(()).iterator(Iter::All).after_tuple(&first[1]).await.expect("rest");
        assert_eq!(rest.iter().map(|u| u.id).collect::<Vec<_>>(), [3, 4, 5]);
    })
    .await;
}

#[tokio::test]
async fn arrow_insert_is_understood_by_the_server() {
    with_space("arrow", |client, space| async move {
        if !client.server_info().supports(tarant::Feature::InsertArrow) {
            eprintln!("skipping: the server has no INSERT_ARROW");
            return;
        }
        // Not a valid IPC stream: the point is that the server parses the
        // request as an Arrow insert and rejects the payload, not the packet.
        let err = client
            .space::<User>(&space)
            .insert_arrow(b"definitely not an arrow stream")
            .await
            .expect_err("garbage is rejected");
        let server = err.as_server().expect("a server-side rejection");
        assert!(server.message.to_lowercase().contains("arrow"), "{server:?}");
    })
    .await;
}

#[tokio::test]
async fn pushes_arrive_before_the_return_value() {
    let Some(client) = client().await else { return };
    let mut call = client
        .eval_with_pushes::<(String,)>(
            "for i = 1, 3 do box.session.push(i * 10) end return 'done'",
            (),
        )
        .await
        .expect("start");
    let mut seen = Vec::new();
    while let Some(value) = call.next_push::<u64>().await.expect("push") {
        seen.push(value);
    }
    assert_eq!(seen, [10, 20, 30]);
    let (result,) = call.finish().await.expect("finish");
    assert_eq!(result, "done");
}
