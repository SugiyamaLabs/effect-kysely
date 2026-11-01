# @sugiyamalabs/effect-kysely

[Kysely](https://kysely.dev)'s type-safe query builder, executed by [Effect 4](https://effect.website)'s `SqlClient`.

Kysely builds and compiles the query. The SQL and its parameters then run through the `SqlClient`
already in your Effect context, so you keep:

- **one connection pool**: whatever `SqlClient` layer you provide (`@effect/sql-pg`, `@effect/sql-sqlite-bun`…);
- **Effect's transactions**: queries inside `sql.withTransaction(...)` roll back with it;
- **Effect's tracing and errors**: failures are `SqlError`, spans come from the client.

`@effect/sql-kysely` covers Effect 3. This package is for Effect 4.

## Install

```sh
bun add @sugiyamalabs/effect-kysely kysely effect
```

## Use

```ts
import { EffectKysely } from "@sugiyamalabs/effect-kysely"
import { PgClient } from "@effect/sql-pg"
import { Effect, Redacted } from "effect"
import { CamelCasePlugin, type Generated } from "kysely"

interface Database {
  users: { id: Generated<number>; displayName: string; createdAt: Generated<Date> }
}

const db = EffectKysely.make<Database>({ dialect: "postgres", plugins: [new CamelCasePlugin()] })

const program = Effect.gen(function*() {
  const ana = yield* db.executeTakeFirstOrFail(
    db.query.insertInto("users").values({ displayName: "Ana" }).returning(["id", "displayName"])
  )
  //    ^ { id: number; displayName: string }
  return yield* db.execute(db.query.selectFrom("users").selectAll().where("id", "=", ana.id))
})

program.pipe(Effect.provide(PgClient.layer({ url: Redacted.make(process.env.DATABASE_URL!) })))
```

### API

`EffectKysely.make<DB>({ dialect, plugins? })` returns:

| | |
|---|---|
| `query` | The Kysely instance to build queries with. It never connects. |
| `execute(q)` | `Effect<ReadonlyArray<Row>, SqlError, SqlClient>` |
| `executeTakeFirst(q)` | First row or `undefined` |
| `executeTakeFirstOrFail(q)` | First row, or fails with `NoSuchElementError` |
| `compile(q)` | The compiled `{ sql, parameters }`, without running it |

- `dialect` is `"postgres" | "sqlite" | "mysql" | "mssql"` and must match the `SqlClient` you provide.
- `q` is any Kysely query builder or a `sql` template.
- Kysely plugins (like `CamelCasePlugin`) work as usual, on both the query and the rows. The client's own
  `transformResultNames` is skipped for these queries, so the rows always match Kysely's types.

### Transactions

```ts
const sql = yield* SqlClient.SqlClient
yield* sql.withTransaction(Effect.gen(function*() {
  yield* db.execute(db.query.insertInto("users").values({ displayName: "Bia" }))
  yield* db.execute(db.query.updateTable("counters").set((e) => ({ n: e("n", "+", 1) })))
}))
```

### Limits

- `numAffectedRows` and `insertId` are not returned, because Effect's `SqlClient` only returns rows. Use
  `returning(...)` where your database supports it.
- Kysely's own `execute()`, `stream()` and `transaction()` don't work on `query`: run everything through
  this package and Effect.

## Develop

```sh
bun install
bun test                                                        # SQLite in memory
PG_URL=postgres://postgres:pw@localhost:5432/postgres bun test  # also against Postgres
bun run check && bun run build
```

MIT © SugiyamaLabs
