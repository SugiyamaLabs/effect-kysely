/**
 * Kysely's type-safe query builder, executed by Effect 4's `SqlClient`.
 *
 * Kysely only builds and compiles the query; the SQL and its parameters run through the
 * `SqlClient` already in your Effect context. That means one connection pool, Effect's
 * transactions (`sql.withTransaction`) and Effect's tracing, with no second database layer.
 *
 * ```ts
 * import { EffectKysely } from "@sugiyamalabs/effect-kysely"
 * import { CamelCasePlugin } from "kysely"
 *
 * const db = EffectKysely.make<Database>({ dialect: "postgres", plugins: [new CamelCasePlugin()] })
 *
 * const program = db.execute(db.query.selectFrom("users").select(["id", "displayName"]).where("id", "=", id))
 * //    ^ Effect<ReadonlyArray<{ id: string; displayName: string }>, SqlError, SqlClient>
 * ```
 */
import { Effect } from "effect"
import type * as Cause from "effect/Cause"
import { SqlClient } from "effect/sql"
import type { SqlError } from "effect/sql/SqlError"
import {
  type Compilable,
  type CompiledQuery,
  type DatabaseIntrospector,
  type Dialect,
  type DialectAdapter,
  DummyDriver,
  Kysely,
  type KyselyPlugin,
  MssqlAdapter,
  MssqlIntrospector,
  MssqlQueryCompiler,
  MysqlAdapter,
  MysqlIntrospector,
  MysqlQueryCompiler,
  PostgresAdapter,
  PostgresIntrospector,
  PostgresQueryCompiler,
  type QueryCompiler,
  type RawBuilder,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
  type UnknownRow
} from "kysely"

/** The SQL flavour Kysely compiles to. Must match the `SqlClient` you provide. */
export type DialectName = "postgres" | "sqlite" | "mysql" | "mssql"

interface Parts {
  readonly adapter: () => DialectAdapter
  readonly compiler: () => QueryCompiler
  readonly introspector: (db: Kysely<any>) => DatabaseIntrospector
}

const DIALECTS: Record<DialectName, Parts> = {
  postgres: {
    adapter: () => new PostgresAdapter(),
    compiler: () => new PostgresQueryCompiler(),
    introspector: (db) => new PostgresIntrospector(db)
  },
  sqlite: {
    adapter: () => new SqliteAdapter(),
    compiler: () => new SqliteQueryCompiler(),
    introspector: (db) => new SqliteIntrospector(db)
  },
  mysql: {
    adapter: () => new MysqlAdapter(),
    compiler: () => new MysqlQueryCompiler(),
    introspector: (db) => new MysqlIntrospector(db)
  },
  mssql: {
    adapter: () => new MssqlAdapter(),
    compiler: () => new MssqlQueryCompiler(),
    introspector: (db) => new MssqlIntrospector(db)
  }
}

/** A Kysely dialect that compiles but never connects: execution belongs to Effect. */
const compileOnly = (name: DialectName): Dialect => {
  const parts = DIALECTS[name]
  return {
    createAdapter: parts.adapter,
    createDriver: () => new DummyDriver(),
    createIntrospector: parts.introspector,
    createQueryCompiler: parts.compiler
  }
}

/** Anything `execute` accepts: query builders (`selectFrom`, `insertInto`…) or `sql` templates. */
export type Query<R> = Compilable<R> | RawBuilder<R>

export interface Options {
  readonly dialect: DialectName
  /**
   * Kysely plugins (for example `CamelCasePlugin`). They rewrite the query when it compiles and the
   * rows when they come back, same as in plain Kysely.
   */
  readonly plugins?: ReadonlyArray<KyselyPlugin>
}

export interface EffectKysely<DB> {
  /** The Kysely instance to build queries with. It can't run them itself: pass them to `execute`. */
  readonly query: Kysely<DB>
  /** Runs the query through the `SqlClient` in context and returns every row. */
  readonly execute: <R>(query: Query<R>) => Effect.Effect<ReadonlyArray<R>, SqlError, SqlClient.SqlClient>
  /** First row, or `undefined`. */
  readonly executeTakeFirst: <R>(query: Query<R>) => Effect.Effect<R | undefined, SqlError, SqlClient.SqlClient>
  /** First row, or fails with `NoSuchElementError`. */
  readonly executeTakeFirstOrFail: <R>(
    query: Query<R>
  ) => Effect.Effect<R, SqlError | Cause.NoSuchElementError, SqlClient.SqlClient>
  /** The compiled SQL and parameters, without running anything (handy in tests and logs). */
  readonly compile: <R>(query: Query<R>) => CompiledQuery<R>
}

const isRawBuilder = (q: Query<unknown>): q is RawBuilder<unknown> =>
  (q as { isRawBuilder?: boolean }).isRawBuilder === true

export const make = <DB>(options: Options): EffectKysely<DB> => {
  const query = new Kysely<DB>({ dialect: compileOnly(options.dialect), plugins: [...(options.plugins ?? [])] })
  const plugins = query.getExecutor().plugins

  const compile = <R>(q: Query<R>): CompiledQuery<R> => (isRawBuilder(q) ? q.compile(query) : q.compile()) as CompiledQuery<R>

  const execute = <R>(q: Query<R>) =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const compiled = compile(q)
      // Without Effect's own name transform: Kysely's plugins own the row shape, so the types stay true.
      let rows = (yield* sql.unsafe<UnknownRow>(compiled.sql, compiled.parameters).withoutTransform) as ReadonlyArray<UnknownRow>
      if (plugins.length > 0) {
        const transformed = yield* Effect.promise(() =>
          plugins.reduce(
            (acc, plugin) => acc.then((result) => plugin.transformResult({ queryId: compiled.queryId, result })),
            Promise.resolve({ rows: [...rows] })
          )
        )
        rows = transformed.rows
      }
      return rows as ReadonlyArray<R>
    })

  const executeTakeFirst = <R>(q: Query<R>) => Effect.map(execute(q), (rows) => rows[0])
  const executeTakeFirstOrFail = <R>(q: Query<R>) => Effect.flatMap(executeTakeFirst(q), Effect.fromNullishOr)

  return { query, execute, executeTakeFirst, executeTakeFirstOrFail, compile }
}

export const EffectKysely = { make }
