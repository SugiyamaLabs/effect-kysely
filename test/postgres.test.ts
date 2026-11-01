/** Against a real Postgres. Runs when PG_URL is set (CI starts one; locally: docker run postgres). */
import { PgClient } from "@effect/sql-pg"
import { describe, expect, test } from "bun:test"
import { Effect, Redacted } from "effect"
import { SqlClient } from "effect/sql"
import { CamelCasePlugin, type Generated, sql } from "kysely"
import { EffectKysely } from "../src/index.ts"

interface Database {
  people: { id: Generated<number>; displayName: string; tags: Array<string>; createdAt: Generated<Date> }
}

const url = process.env.PG_URL
const db = EffectKysely.make<Database>({ dialect: "postgres", plugins: [new CamelCasePlugin()] })

const run = <A, E>(effect: Effect.Effect<A, E, SqlClient.SqlClient>) =>
  Effect.runPromise(
    Effect.gen(function*() {
      const s = yield* SqlClient.SqlClient
      yield* s`DROP TABLE IF EXISTS people`
      yield* s`CREATE TABLE people (id SERIAL PRIMARY KEY, display_name TEXT NOT NULL, tags TEXT[] NOT NULL DEFAULT '{}', created_at TIMESTAMPTZ NOT NULL DEFAULT now())`
      return yield* effect
    }).pipe(Effect.scoped, Effect.provide(PgClient.layer({ url: Redacted.make(url!) })))
  )

describe.skipIf(!url)("postgres", () => {
  test("$1 placeholders, returning, arrays, dates and camelCase rows", async () => {
    const out = await run(Effect.gen(function*() {
      const ana = yield* db.executeTakeFirstOrFail(
        db.query.insertInto("people").values({ displayName: "Ana", tags: ["mod", "voz"] }).returningAll()
      )
      const found = yield* db.execute(
        db.query.selectFrom("people").select(["id", "displayName"]).where(sql<boolean>`${sql.ref("tags")} @> ${sql.val(["voz"])}`)
      )
      return { ana, found }
    }))
    expect(db.compile(db.query.selectFrom("people").selectAll().where("id", "=", 1)).sql).toBe(
      'select * from "people" where "id" = $1'
    )
    expect(out.ana.displayName).toBe("Ana")
    expect(out.ana.tags).toEqual(["mod", "voz"])
    expect(out.ana.createdAt).toBeInstanceOf(Date)
    expect(out.found).toEqual([{ id: out.ana.id, displayName: "Ana" }])
  })

  test("rollback inside Effect's transaction", async () => {
    const n = await run(Effect.gen(function*() {
      const s = yield* SqlClient.SqlClient
      yield* s.withTransaction(
        Effect.andThen(db.execute(db.query.insertInto("people").values({ displayName: "Bia", tags: [] })), Effect.fail("boom"))
      ).pipe(Effect.ignore)
      const r = yield* db.executeTakeFirstOrFail(db.query.selectFrom("people").select((e) => e.fn.countAll<string>().as("n")))
      return Number(r.n)
    }))
    expect(n).toBe(0)
  })
})
