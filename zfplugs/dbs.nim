#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import db_postgres, db_mysql, db_sqlite, strformat, json, strutils, strformat, options
import stdext/[strutils_ext]
export db_postgres, db_mysql, db_sqlite

type
  MySql* = db_mysql.DbConn
  PgSql* = db_postgres.DbConn
  SqLite* = db_sqlite.DbConn

  Dbs*[T] = ref object
    database: string
    username: string
    password: string
    host: string
    port: int

proc newDbs*[T](
  database: string,
  username: string = "",
  password: string = "",
  host: string = "",
  port: int = 0): Dbs[T] {.gcsafe.} =
  let instance = Dbs[T](
    database: database,
    username: username,
    password: password,
    host: host,
    port: port
  )

  return instance

proc tryConnect*[T](self: Dbs[T]): tuple[success: bool, conn: T, msg: string] {.gcsafe.} =
  ##
  ## Try connect to database
  ## Generic T is type of MySql, PgSql, SqLite
  ##
  try:
    if T is PgSql:
      result = (
        true,
        cast[T](db_postgres.open(
          &"{self.host}:{self.port}",
          self.username,
          self.password,
          self.database)),
        "OK")
    elif T is MySql:
      result = (
        true,
        cast[T](db_mysql.open(
          &"{self.host}:{self.port}",
          self.username,
          self.password,
          self.database)),
        "OK")
    elif T is SqLite:
      result = (
        true,
        cast[T](db_sqlite.open(
          self.database,
          "",
          "",
          "")),
        "OK")
    else:
      let dbType = $(type T)
      raise newException(ObjectConversionDefect, &"unknown database type {dbType}")
  except Exception as ex:
    result = (false, nil, ex.msg)

proc tryCheckConnect*[T](self: Dbs[T]): tuple[success: bool, msg: string] {.gcsafe.} =
  try:
    var c = self.tryConnect[T]()
    if c.success:
      c.conn.get().close
    return (true, "OK")
  except Exception as ex:
    result = (false, ex.msg)

