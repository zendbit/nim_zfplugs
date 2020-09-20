#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import db_postgres, db_mysql, db_sqlite, strformat, json, strutils
import stdext/[strutilsExt]

type
  Dbs* = ref object
    database: string
    username: string
    password: string
    host: string
    port: int

proc newDbs*(
  database: string,
  username: string = "",
  password: string = "",
  host: string = "",
  port: int = 0): Dbs =
  let instance = Dbs(
    database: database,
    username: username,
    password: password,
    host: host,
    port: port
  )

  return instance

proc tryPgSqlConn*(self: Dbs): tuple[success: bool, conn: db_postgres.DbConn, msg: string] =
    try:
      result = (
        true,
          db_postgres.open(
          &"{self.host}:{self.port}",
          self.username,
          self.password,
          self.database),
        "OK")
    except Exception as ex:
      result = (false, nil, ex.msg)

proc tryPgSqlCheck*(self: Dbs): tuple[success: bool, msg: string] =
    try:
      let c = self.tryPgSqlConn()
      if c.success:
        c.conn.close()
      return (true, "OK")
    except Exception as ex:
      result = (false, ex.msg)

proc tryMySqlConn*(self: Dbs): tuple[success: bool, conn: db_mysql.DbConn, msg: string] =
    try:
      result = (
        true,
        db_mysql.open(
          &"{self.host}:{self.port}",
          self.username,
          self.password,
          self.database),
        "OK")
    except Exception as ex:
      result = (false, nil, ex.msg)

proc tryMySqlCheck*(self: Dbs): tuple[success: bool, msg: string] =
    try:
      let c = self.tryMySqlConn()
      if c.success:
        c.conn.close()
      return (true, "OK")
    except Exception as ex:
      result = (false, ex.msg)

proc trySqliteConn*(self: Dbs): tuple[success: bool, conn: db_sqlite.DbConn, msg: string] =
  try:
    result = (
      true,
      db_sqlite.open(
        self.database,
        "",
        "",
        ""),
      "OK")
  except Exception as ex:
    result = (false, nil, ex.msg)

proc trySqliteCheck*(self: Dbs): tuple[success: bool, msg: string] =
    try:
      let c = self.trySqliteConn()
      if c.success:
        c.conn.close()
      return (true, "OK")
    except Exception as ex:
      result = (false, ex.msg)

proc toDbType*(field: string, value: string): JsonNode =
  let data = field.split(":")
  result = %*{data[0]: nil}
  if data.len == 2:
    if value != "":
      case data[1]
      of "int":
        result[data[0]] = %value.tryParseInt().val
      of "uInt":
        result[data[0]] = %value.tryParseUInt().val
      of "bigInt":
        result[data[0]] = %value.tryParseBiggestInt().val
      of "bigUInt":
        result[data[0]] = %value.tryParseBiggestUInt().val
      of "float":
        result[data[0]] = %value.tryParseFloat().val
      of "bigFloat":
        result[data[0]] = %value.tryParseBiggestFloat().val
      of "bool":
        result[data[0]] = %value.tryParseBool().val
  elif value != "":
    result[data[0]] = %value

proc toDbType*(field: string, nodeKind: JsonNodeKind, value: string): JsonNode =
  result = %*{field: nil}
  if value != "":
    case nodeKind
    of JInt:
      result[field] = %value.tryParseBiggestInt().val
    of JFloat:
      result[field] = %value.tryParseFloat().val
    of JBool:
      result[field] = %value.tryParseBool().val
    else:
      result[field] = %value
