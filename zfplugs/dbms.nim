#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import strformat, strutils, sequtils, json, options
import stdext.json_ext
import dbs, settings, dbssql
export dbs, dbs_sql

type
  DbInfo* = tuple[
    database: string,
    username: string,
    password: string,
    host: string, port: int]

  DBMS*[T] = ref object
    connId: string
    dbInfo: DbInfo
    conn: T
    connected: bool

#var db: DBConn

#
# this will read the settings.json on the section
# "database": {
#   "your_connId_setting": {
#     "username": "",
#     "password": "",
#     "database": "",
#     "host": "",
#     "port": 1234
#   }
# }
#
proc newDBMS*[T](connId: string): DBMS[T] =
  let jsonSettings = jsonSettings()
  if not jsonSettings.isNil:
    let db = jsonSettings{"database"}
    if not db.isNil:
      let dbConf = db{connId}
      if not dbConf.isNil:
        result = DBMS[T](connId: connId)
        result.dbInfo = (
          dbConf{"database"}.getStr(),
          dbConf{"username"}.getStr(),
          dbConf{"password"}.getStr(),
          dbConf{"host"}.getStr(),
          dbConf{"port"}.getInt())
        let c = newDbs[T](
          result.dbInfo.database,
          result.dbInfo.username,
          result.dbInfo.password,
          result.dbInfo.host,
          result.dbInfo.port).tryConnect()

        result.connected = c.success
        if c.success:
          result.conn = c.conn
        else:
          echo c.msg

      else:
        echo &"database {connId} not found!!."

    else:
      echo "database section not found!!."

proc extractKeyValue[T](
  self: DBMS,
  obj: T): tuple[keys: seq[string], values: seq[string]] =
  var keys: seq[string] = @[]
  var values: seq[string] = @[]
  let obj = %obj
  for k, v in obj.discardNull:
    if k.toLower.contains("-as-"): continue
    
    var skip = false
    for kf in obj.keys:
      if kf.toLower.endsWith(&"as-{k}"):
        skip = true
        break
    if skip: continue

    keys.add(k)
    if v.kind != JString:
      values.add($v)
    else:
      values.add(v.getStr)

  return (keys, values)

# insert into database
proc insertId*[T](
  self: DBMS,
  table: string,
  obj: T): tuple[ok: bool, insertId: int64, msg: string] =
  try:
    let kv = self.extractKeyValue(obj)
    let q = Sql()
      .insert(table, kv.keys)
      .value(kv.values).toQ
    
    return (true,
      self.conn.insertId(q.query, q.params),
      "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc update*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): tuple[ok: bool, affected: int64, msg: string] =
  ### update data table
  try:
    let kv = self.extractKeyValue(obj)
    let q = (Sql()
      .update(table, kv.keys)
      .value(kv.values) & query).toQ
     
    return (true,
      self.conn.execAffectedRows(q.query, q.params),
      "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc dbError*(self: DBMS) =
  ###
  ### Raise DbError exception
  ###
  self.conn.dbError

proc dbQuote*(s: string): string =
  ###
  ### Database string quote
  ###
  return dbQuote(s)

proc exec*(
  self: DBMS,
  query: Sql): tuple[ok: bool, msg: string] =
  ###
  ### execute the query
  ###
  try:
    if not self.connected:
      return (false, "can't connect to the database.")
    let q = query.toQ
    self.conn.exec(q.query, q.params)
    return (true, "ok")
  except Exception as ex:
    return (false, ex.msg)

proc extractFieldsAlias(fields: seq[FieldDesc]): seq[FieldDesc] =
  let fields = fields.map(proc (x: FieldDesc): FieldDesc =
    (x.name.replace("-as-", " AS ").replace("-AS-", " AS "), x.nodeKind))
  
  return fields.filter(proc (x: FieldDesc): bool =
    result = true
    if not x.name.contains("AS "):
      for f in fields:
        if f.name.contains(&" AS {x.name}"):
          result = false
          break)

proc extractQueryResults(fields: seq[FieldDesc], queryResults: seq[string]): JsonNode =
  result = %*{}
  if queryResults.len > 0 and queryResults[0] != "":
    for i in 0..fields.high:
      for k, v in fields[i].name.toDbType(fields[i].nodeKind, queryResults[i]):
        var fprops = k.split(" AS ")
        result[fprops[fprops.high].strip] = v

proc getRow*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): tuple[ok: bool, row: T, msg: string] =

  try:
    if not self.connected:
      return (false, obj, "can't connect to the database.")

    let fields = extractFieldsAlias(obj.fieldsDesc)
    let q = (Sql()
      .select(fields.map(proc(x: FieldDesc): string = x.name))
      .fromTable(table) & query).toQ
    
    let queryResults = self.conn.getRow(q.query, q.params)
    return (true, extractQueryResults(fields, queryResults).to(T), "ok")
  except Exception as ex:
    return (false, obj, ex.msg)

proc getAllRows*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): tuple[ok: bool, rows: seq[T], msg: string] =
  ###
  ### Retrieves a single row. If the query doesn't return any rows,
  ###
  try:
    if not self.connected:
      return (false, @[], "can't connect to the database.")
    
    let fields = extractFieldsAlias(obj.fieldsDesc)
    let q = (Sql()
      .select(fields.map(proc(x: FieldDesc): string = x.name))
      .fromTable(table) & query).toQ

    let queryResults = self.conn.getAllRows(q.query, q.params)
    var res: seq[T] = @[]
    if queryResults.len > 0 and queryResults[0][0] != "":
      for qres in queryResults:
        res.add(extractQueryResults(fields, qres).to(T))
    return (true, res, "ok")
  except Exception as ex:
    return (false, @[], ex.msg)

proc execAffectedRows*(
  self: DBMS,
  query: Sql): tuple[ok: bool, affected: int64, msg: string] =
  ###
  ### runs the query (typically "UPDATE") and returns the number of affected rows
  ###
  try:
    if not self.connected:
      return (false, 0'i64, "can't connect to the database.")
    let q = query.toQ
    return (true, self.conn.execAffectedRows(q.query, q.params), "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc delete*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): tuple[ok: bool, affected: int64, msg: string] =
  ###
  ### runs the query delete and returns the number of affected rows
  ###
  try:
    if not self.connected:
      return (false, 0'i64, "can't connect to the database.")

    let q = (Sql()
      .delete(table) & query).toQ
    return (true, self.conn.execAffectedRows(q.query, q.params), "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc setEncoding(
  self: DBMS,
  encoding: string): bool =
  ###
  ### sets the encoding of a database connection, returns true for success, false for failure
  ###
  if not self.connected:
    return false
  return self.conn.setEncoding(encoding)

proc getDbInfo*(self: DBMS): DbInfo =
  return self.dbInfo

# close the database connection
proc close*(self: DBMS) =
  try:
    self.conn.close
  except:
    discard
  self.connected = false

# test ping the server
proc ping*(self: DBMS): bool =
  try:
    if not self.connected:
      return self.connected
    self.conn.exec(sql "SELECT 1")
    result = true
  except:
    self.close
    discard

# get connId
proc connId*(self: DBMS): string =
  if not self.isNil:
    result = self.connId

