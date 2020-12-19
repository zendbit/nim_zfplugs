#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import strformat, strutils, sequtils, json, options, re
import stdext.json_ext
import dbs, settings, dbssql
export dbs, dbssql

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
proc newDBMS*[T](connId: string): DBMS[T] {.gcsafe.} =
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

proc quote(str: string): string =
  return (fmt"{str}")
    .replace(fmt"\", fmt"\\")
    .replace(fmt"'", fmt"\'")
    .replace(fmt""""""", fmt"""\"""")
    .replace(fmt"\x1a", fmt"\\Z")

proc extractKeyValue*[T](
  self: DBMS,
  obj: T): tuple[keys: seq[string], values: seq[string], nodesKind: seq[JsonNodeKind]] {.gcsafe.} =
  var keys: seq[string] = @[]
  var values: seq[string] = @[]
  var nodesKind: seq[JsonNodeKind] = @[]
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
    nodesKind.add(v.kind)
    if v.kind != JString:
      values.add($v)
    else:
      values.add(v.getStr)

  return (keys, values, nodesKind)

proc quote(q: Sql): string =
  let q = q.toQs
  var queries = q.query.split("?")
  for i in 0..q.params.high:
    let p = q.params[i]
    let v = if p.nodeKind == JString: &"'{quote(p.val)}'" else: p.val
    queries.insert([v], (i*2) + 1)

  return queries.join("")

# insert into database
proc insertId*[T](
  self: DBMS,
  table: string,
  obj: T): tuple[ok: bool, insertId: int64, msg: string] {.gcsafe.} =
  
  var q = Sql()
  try:
    let kv = self.extractKeyValue(obj)
    var fieldItems: seq[FieldItem] = @[]
    for i in 0..kv.keys.high:
      fieldItems.add((kv.values[i], kv.nodesKind[i]))

    #q = Sql()
    #  .insert(table, kv.keys)
    #  .value(kv.values).toQs
    q = Sql()
      .insert(table, kv.keys)
      .value(fieldItems)
    return (true,
      #self.conn.insertId(sql q.query, q.params),
      self.conn.insertId(sql quote(q)),
      "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo quote(q)
    return (false, 0'i64, ex.msg)

proc update*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): tuple[ok: bool, affected: int64, msg: string] {.gcsafe.} =
  ### update data table

  var q = Sql()
  try:
    let kv = self.extractKeyValue(obj)
    var fieldItems: seq[FieldItem] = @[]
    for i in 0..kv.keys.high:
      fieldItems.add((kv.values[i], kv.nodesKind[i]))
    #q = (Sql()
    #  .update(table, kv.keys)
    #  .value(kv.values) & query).toQs
    q = (Sql()
      .update(table, kv.keys)
      .value(fieldItems) & query)
     
    return (true,
      #self.conn.execAffectedRows(sql q.query, q.params),
      self.conn.execAffectedRows(sql quote(q)),
      "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo quote(q)
    return (false, 0'i64, ex.msg)

proc exec*(
  self: DBMS,
  query: Sql): tuple[ok: bool, msg: string] {.gcsafe.} =
  ###
  ### execute the query
  ###

  var q = Sql()
  try:
    if not self.connected:
      return (false, "can't connect to the database.")
    q = query
    #self.conn.exec(sql q.query, q.params)
    self.conn.exec(sql quote(q))
    return (true, "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo quote(q)
    return (false, ex.msg)

proc extractFieldsAlias*(fields: seq[FieldDesc]): seq[FieldDesc] {.gcsafe.} =
  
  let fields = fields.map(proc (x: FieldDesc): FieldDesc =
    (x.name.replace("-as-", " AS ").replace("-AS-", " AS "), x.nodeKind))
  
  return fields.filter(proc (x: FieldDesc): bool =
    result = true
    if not x.name.contains("AS "):
      for f in fields:
        if f.name.contains(&" AS {x.name}"):
          result = false
          break)

proc normalizeFieldsAlias*(fields: seq[FieldDesc]): seq[FieldDesc] {.gcsafe.} =
  
  return fields.extractFieldsAlias.map(proc (x: FieldDesc): FieldDesc =
    (x.name.split(" AS ")[0].strip, x.nodeKind))

proc extractQueryResults*(fields: seq[FieldDesc], queryResults: seq[string]): JsonNode {.gcsafe.} =
  
  result = %*{}
  if queryResults.len > 0 and queryResults[0] != "" and queryResults.len == fields.len:
    for i in 0..fields.high:
      for k, v in fields[i].name.toDbType(fields[i].nodeKind, queryResults[i]):
        var fprops = k.split(" AS ")
        result[fprops[fprops.high].strip] = v

proc getRow*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): tuple[ok: bool, row: T, msg: string] {.gcsafe.} =

  var q = Sql()
  try:
    if not self.connected:
      return (false, obj, "can't connect to the database.")

    let fields = extractFieldsAlias(obj.fieldsDesc)
    q = (Sql()
      .select(fields.map(proc(x: FieldDesc): string = x.name))
      .fromTable(table) & query)
     
    #let queryResults = self.conn.getRow(sql q.query, q.params)
    let queryResults = self.conn.getRow(sql quote(q))
    return (true, extractQueryResults(fields, queryResults).to(T), "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo quote(q)
    return (false, obj, ex.msg)

proc getAllRows*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): tuple[ok: bool, rows: seq[T], msg: string] {.gcsafe.} =
  ###
  ### Retrieves a single row. If the query doesn't return any rows,
  ###

  var q = Sql()
  try:
    if not self.connected:
      return (false, @[], "can't connect to the database.")
    
    let fields = extractFieldsAlias(obj.fieldsDesc)
    q = (Sql()
      .select(fields.map(proc(x: FieldDesc): string = x.name))
      .fromTable(table) & query)

    #let queryResults = self.conn.getAllRows(sql q.query, q.params)
    let queryResults = self.conn.getAllRows(sql quote(q))
    var res: seq[T] = @[]
    if queryResults.len > 0 and queryResults[0][0] != "":
      for qres in queryResults:
        res.add(extractQueryResults(fields, qres).to(T))
    return (true, res, "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo quote(q)
    return (false, @[], ex.msg)

proc execAffectedRows*(
  self: DBMS,
  query: Sql): tuple[ok: bool, affected: int64, msg: string] {.gcsafe.} =
  ###
  ### runs the query (typically "UPDATE") and returns the number of affected rows
  ###

  var q = Sql()
  try:
    if not self.connected:
      return (false, 0'i64, "can't connect to the database.")
    q = query

    #return (true, self.conn.execAffectedRows(sql q.query, q.params), "ok")
    return (true, self.conn.execAffectedRows(sql quote(q)), "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo quote(q)
    return (false, 0'i64, ex.msg)

proc delete*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): tuple[ok: bool, affected: int64, msg: string] {.gcsafe.} =
  ###
  ### runs the query delete and returns the number of affected rows
  ###
  
  var q = Sql()
  try:
    if not self.connected:
      return (false, 0'i64, "can't connect to the database.")

    q = (Sql()
      .delete(table) & query)
    
    #return (true, self.conn.execAffectedRows(sql q.query, q.params), "ok")
    return (true, self.conn.execAffectedRows(sql quote(q)), "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo quote(q)
    return (false, 0'i64, ex.msg)

proc setEncoding(
  self: DBMS,
  encoding: string): bool {.gcsafe.} =
  ###
  ### sets the encoding of a database connection, returns true for success, false for failure
  ###
  if not self.connected:
    return false
  return self.conn.setEncoding(encoding)

proc getDbInfo*(self: DBMS): DbInfo {.gcsafe.} =
  return self.dbInfo

# close the database connection
proc close*(self: DBMS) {.gcsafe.} =
  try:
    self.conn.close
  except:
    discard
  self.connected = false

# test ping the server
proc ping*(self: DBMS): bool {.gcsafe.} =
  try:
    if not self.connected:
      return self.connected
    self.conn.exec(sql "SELECT 1")
    result = true
  except:
    self.close
    discard

# get connId
proc connId*(self: DBMS): string {.gcsafe.} =
  if not self.isNil:
    result = self.connId

