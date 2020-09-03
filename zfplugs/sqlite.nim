#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import db_sqlite, strformat, json

import dbs, settings

type
  DbInfo* = tuple[database: string, username: string, password: string, host: string, port: int]
  SqLite* = ref object
    connId: string
    dbInfo: DbInfo
    conn: DbConn
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
proc newSqLite*(connId: string): SqLite =
  let jsonSettings = jsonSettings()
  if not jsonSettings.isNil:
    let db = jsonSettings{"database"}
    if not db.isNil:
      let dbConf = db{connId}
      if not dbConf.isNil:
        result = SqLite(connId: connId)
        result.dbInfo = (
          dbConf{"database"}.getStr(),
          dbConf{"username"}.getStr(),
          dbConf{"password"}.getStr(),
          dbConf{"host"}.getStr(),
          dbConf{"port"}.getInt())
        let c = newDbs(
          result.dbInfo.database,
          result.dbInfo.username,
          result.dbInfo.password,
          result.dbInfo.host,
          result.dbInfo.port).trySqLiteConn()

        result.connected = c.success
        if c.success:
          result.conn = c.conn
        else:
          echo c.msg

      else:
        echo &"database {connId} not found!!."

    else:
      echo "database section not found!!."

# insert into database
proc insertId*(
  self: SqLite, tbl: string, keyValues: openArray[tuple[k: string, v: string]]
  ): tuple[ok: bool, insertId: int64, msg: string] =
  try:
    var sqlStr = "INSERT INTO"
    var keys: seq[string] = @[]
    var values: seq[string] = @[]
    var valuesParam: seq[string] = @[]
    for kv in keyValues:
      let k = kv[0]
      let v = kv[1]
      keys.add(k)
      if v.toLower == "nil" or
        v.toLower == "null" or
        v.toLower == ($dbNull).toLower:
         values.add("NULL")
      else:
        values.add("?")
        valuesParam.add(v)

    return (true,
      self.conn.insertId(sql &"""{sqlStr} {tbl} ({keys.join(",")}) VALUES ({values.join(",")})""", valuesParam),
      "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc update*(
  self: SqLite, tbl: string, keyValues: openArray[tuple[k: string, v: string]],
  stmt: string, params: varargs[string, `$`]): tuple[ok: bool, affected: int64, msg: string] =
  ### update data table
  try:
    var sqlStr = "UPDATE"
    var setVal: seq[string] = @[]
    var setValParam: seq[string] = @[]
    for kv in keyValues:
      let k = kv[0]
      let v = kv[1]
      var setValKV: seq[string] = @[]
      setValKV.add(k)
      if v.toLower == "nil" or
        v.toLower == "null" or
        v.toLower == ($dbNull).toLower:
         setValKV.add("NULL")
      else:
        setValKV.add("?")
        setValParam.add(v)
      
      setVal.add(setValKV.join("="))

    # add where param
    for wParams in params:
      setValParam.add(wParams)

    return (true,
      self.conn.execAffectedRows(sql &"""{sqlStr} {tbl} SET {setVal.join(",")} WHERE {stmt}""", setValParam),
      "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc dbError*(self: SqLite) =
  ###
  ### Raise DbError exception
  ###
  self.conn.dbError

proc dbQuote*(s: string): string =
  ###
  ### Database string quote
  ###
  return dbQuote(s)

proc exec*(self: SqLite, query: string, args: varargs[string, `$`]): tuple[ok: bool, msg: string] =
  ###
  ### execute the query
  ###
  try:
    if not self.connected:
      return (false, "can't connect to the database.")
    self.conn.exec(sql query, args)
    return (true, "ok")
  except Exception as ex:
    return (false, ex.msg)

proc getRow*(self: SqLite, tbl: string, fields: openArray[string],
  stmt: string = "", params: varargs[string, `$`] = []): tuple[ok: bool, row: JsonNode, msg: string] =
  ###
  ### Retrieves a single row. If the query doesn't return any rows,
  ### this proc will return a Row with empty strings for each column
  ###
  try:
    if not self.connected:
      return (false, nil, "can't connect to the database.")
    let f = fields.map(proc (x: string): string = (x.split(":"))[0]).join(",")
    let sqlStr = &"""SELECT {f} FROM"""
    let queryResult = self.conn.getRow(sql &"{sqlStr} {tbl} {stmt}", params)
    var res = %*{}
    if queryResult[0] != "":
      for i in 0..fields.high:
        for k, v in fields[i].toDbType(queryResult[i]):
          var fname = k.toLower()
          if fname.contains(" as "):
            fname = fname.split(" as ")[1].strip
          res[fname] = v
    return (true, res, "ok")
  except Exception as ex:
    return (false, nil, ex.msg)

proc getAllRows*(self: SqLite, tbl: string, fields: openArray[string],
  stmt: string = "", params: varargs[string, `$`] = []): tuple[ok: bool, rows: JsonNode, msg: string] =
  ###
  ### Retrieves a single row. If the query doesn't return any rows,
  ### this proc will return a Row with empty strings for each column
  ###
  try:
    if not self.connected:
      return (false, nil, "can't connect to the database.")
    let f = fields.map(proc (x: string): string = (x.split(":"))[0]).join(",")
    let sqlStr = &"""SELECT {f} FROM"""
    let queryResult = self.conn.getAllRows(sql &"{sqlStr} {tbl} {stmt}", params)
    var res: seq[JsonNode] = @[]
    if queryResult[0][0] != "":
      for qres in queryResult:
        var resItem = %*{}
        for i in 0..fields.high:
          for k, v in fields[i].toDbType(qres[i]):
            var fname = k.toLower()
            if fname.contains(" as "):
              fname = fname.split(" as ")[1].strip
            resItem[fname] = v
        res.add(resItem)
    return (true, %res, "ok")
  except Exception as ex:
    return (false, nil, ex.msg)

proc execAffectedRows*(
  self: SqLite, query: string, args: varargs[string, `$`]): tuple[ok: bool, affected: int64, msg: string] =
  ###
  ### runs the query (typically "UPDATE") and returns the number of affected rows
  ###
  try:
    if not self.connected:
      return (false, 0'i64, "can't connect to the database.")
    return (true, self.conn.execAffectedRows(sql query, args), "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc setEncoding(self: SqLite, encoding: string): bool =
  ###
  ### sets the encoding of a database connection, returns true for success, false for failure
  ###
  if not self.connected:
    return false
  return self.conn.setEncoding(encoding)

proc getDbInfo*(self: SqLite): DbInfo =
  return self.dbInfo

# close the database connection
proc close*(self: SqLite) =
  try:
    self.conn.close
  except:
    discard
  self.connected = false

# test ping the server
proc ping*(self: SqLite): bool =
  try:
    if not self.connected:
      return self.connected
    self.conn.exec(sql "SELECT 1")
    result = true
  except:
    self.close
    discard

# get connId
proc connId*(self: SqLite): string =
  if not self.isNil:
    result = self.connId

export
  db_sqlite
