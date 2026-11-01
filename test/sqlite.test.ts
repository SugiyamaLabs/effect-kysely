import { SqliteClient } from "@effect/sql-sqlite-bun"
import { describe, expect, test } from "bun:test"
import { Effect, Exit } from "effect"
import { SqlClient } from "effect/sql"
import { CamelCasePlugin, type Generated, sql } from "kysely"
import { EffectKysely } from "../src/index.ts"

interface Database {
  users: { id: Generated<number>; display_name: string; created_at: number }
  posts: { id: Generated<number>; user_id: number; title: string }
}

interface CamelDatabase {
  users: { id: Generated<number>; displayName: string; createdAt: number }
}

const db = EffectKysely.make<Database>({ dialect: "sqlite" })

const run = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.runPromise(
    Effect.gen(function*() {
      const s = yield* SqlClient.SqlClient
      yield* s`CREATE TABLE users (id INTEGER PRIMARY KEY, display_name TEXT NOT NULL, created_at INTEGER NOT NULL)`
      yield* s`CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, title TEXT NOT NULL)`
      return yield* effect
    }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })))
  )

describe("execute", () => {
  test("insert with returning, select, join and update", async () => {
    const out = await run(Effect.gen(function*() {
      const ana = yield* db.executeTakeFirstOrFail(
        db.query.insertInto("users").values({ display_name: "Ana", created_at: 1 }).returning(["id", "display_name"])
      )
      yield* db.execute(db.query.insertInto("users").values({ display_name: "Bia", created_at: 2 }))
      yield* db.execute(db.query.insertInto("posts").values({ user_id: ana.id, title: "Oi" }))
      yield* db.execute(db.query.updateTable("users").set({ display_name: "Ana Paula" }).where("id", "=", ana.id))
      const rows = yield* db.execute(
        db.query.selectFrom("posts").innerJoin("users", "users.id", "posts.user_id").select(["users.display_name", "posts.title"])
      )
      const all = yield* db.execute(db.query.selectFrom("users").selectAll().orderBy("created_at"))
      return { ana, rows, all }
    }))
    expect(out.ana).toEqual({ id: 1, display_name: "Ana" })
    expect(out.rows).toEqual([{ display_name: "Ana Paula", title: "Oi" }])
    expect(out.all.map((u) => u.display_name)).toEqual(["Ana Paula", "Bia"])
  })

  test("takeFirst: undefined when empty; orFail fails with NoSuchElementError", async () => {
    const first = await run(db.executeTakeFirst(db.query.selectFrom("users").selectAll()))
    expect(first).toBeUndefined()
    const exit = await Effect.runPromiseExit(
      Effect.gen(function*() {
        const s = yield* SqlClient.SqlClient
        yield* s`CREATE TABLE users (id INTEGER PRIMARY KEY, display_name TEXT NOT NULL, created_at INTEGER NOT NULL)`
        return yield* db.executeTakeFirstOrFail(db.query.selectFrom("users").selectAll())
      }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:" })))
    )
    expect(Exit.isFailure(exit)).toBe(true)
    expect(JSON.stringify(exit)).toContain("NoSuchElementError")
  })

  test("raw sql templates run too", async () => {
    const rows = await run(Effect.gen(function*() {
      yield* db.execute(db.query.insertInto("users").values({ display_name: "Ana", created_at: 1 }))
      return yield* db.execute(sql<{ n: number }>`SELECT count(*) AS n FROM users WHERE display_name = ${"Ana"}`)
    }))
    expect(rows).toEqual([{ n: 1 }])
  })

  test("runs inside Effect's transaction: a failure rolls the Kysely write back", async () => {
    const count = await run(Effect.gen(function*() {
      const s = yield* SqlClient.SqlClient
      yield* s.withTransaction(Effect.gen(function*() {
        yield* db.execute(db.query.insertInto("users").values({ display_name: "Ana", created_at: 1 }))
        return yield* Effect.fail("boom")
      })).pipe(Effect.ignore)
      return yield* db.executeTakeFirstOrFail(db.query.selectFrom("users").select((e) => e.fn.countAll<number>().as("n")))
    }))
    expect(count.n).toBe(0)
  })

  test("plugins rewrite the query and the rows (CamelCasePlugin)", async () => {
    const camel = EffectKysely.make<CamelDatabase>({ dialect: "sqlite", plugins: [new CamelCasePlugin()] })
    expect(camel.compile(camel.query.selectFrom("users").select("displayName")).sql).toBe('select "display_name" from "users"')
    const row = await run(Effect.gen(function*() {
      yield* camel.execute(camel.query.insertInto("users").values({ displayName: "Ana", createdAt: 5 }))
      return yield* camel.executeTakeFirstOrFail(camel.query.selectFrom("users").select(["displayName", "createdAt"]))
    }))
    expect(row).toEqual({ displayName: "Ana", createdAt: 5 })
  })

  test("Effect's own name transform is skipped, so the row matches Kysely's types", async () => {
    const row = await Effect.runPromise(
      Effect.gen(function*() {
        const s = yield* SqlClient.SqlClient
        yield* s`CREATE TABLE users (id INTEGER PRIMARY KEY, display_name TEXT NOT NULL, created_at INTEGER NOT NULL)`
        yield* db.execute(db.query.insertInto("users").values({ display_name: "Ana", created_at: 1 }))
        return yield* db.executeTakeFirstOrFail(db.query.selectFrom("users").select("display_name"))
      }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", transformResultNames: (s) => s.toUpperCase() })))
    )
    expect(row).toEqual({ display_name: "Ana" })
  })
})
