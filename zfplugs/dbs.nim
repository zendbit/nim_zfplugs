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

proc newDbs2*[T](
  database: string,
  username: string = "",
  password: string = "",
  host: string = "",
  port: int = 0): Dbs[T] =
  let instance = Dbs[T](
    database: database,
    username: username,
    password: password,
    host: host,
    port: port
  )

  return instance

proc tryConnect*[T](self: Dbs[T]): tuple[success: bool, conn: T, msg: string] =
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
      raise newException(ObjectConversionError, &"unknown database type {dbType}")
  except Exception as ex:
    result = (false, nil, ex.msg)

proc tryCheckConnect*[T](self: Dbs[T]): tuple[success: bool, msg: string] =
  try:
    var c = self.tryConnect[T]()
    if c.success:
      c.conn.get().close
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

proc toWhereQuery*(j: JsonNode, op: string = "AND"): tuple[where: string, params: seq[string]] =
  var where: seq[string] = @[]
  var whereParams: seq[string] = @[]
  for k, v in j:
    where.add(&"{k}=?")
    whereParams.add(if v.kind == JString: v.getStr else: $v)

  return (where.join(&" {op} "), whereParams)

