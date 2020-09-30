#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import strformat, strutils, sequtils, json, options
import stdext/[json_ext]
import dbs, settings
export dbs

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
        let c = newDbs2[T](
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

# insert into database
proc insertId*(
  self: DBMS,
  tbl: string,
  data: JsonNode): tuple[ok: bool, insertId: int64, msg: string] =
  try:
    var sqlStr = "INSERT INTO"
    var keys: seq[string] = @[]
    var values: seq[string] = @[]
    var valuesParam: seq[string] = @[]
    for k, v in data:
      keys.add(k)
      if v.kind == JNUll:
         values.add("NULL")
      else:
        values.add("?")
        if v.kind != JString:
          valuesParam.add($v)
        else:
          valuesParam.add(v.getStr)
    
    return (true,
      self.conn.insertId(sql &"""{sqlStr} {tbl} ({keys.join(",")}) VALUES ({values.join(",")})""", valuesParam),
      "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc update*(
  self: DBMS,
  tbl: string,
  data: JsonNode,
  stmt: string, params: varargs[string, `$`]): tuple[ok: bool, affected: int64, msg: string] =
  ### update data table
  try:
    var sqlStr = "UPDATE"
    var setVal: seq[string] = @[]
    var setValParam: seq[string] = @[]
    for k, v in data:
      var setValKV: seq[string] = @[]
      setValKV.add(k)
      if v.kind == JNull:
         setValKV.add("NULL")
      else:
        setValKV.add("?")
        if v.kind != JString:
          setValParam.add($v)
        else:
          setValParam.add(v.getStr)
      
      setVal.add(setValKV.join("="))

    # add where param
    for wParams in params:
      setValParam.add(wParams)
    
    return (true,
      self.conn.execAffectedRows(sql &"""{sqlStr} {tbl} SET {setVal.join(",")} {stmt}""", setValParam),
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
  query: string,
  args: varargs[string, `$`] = []): tuple[ok: bool, msg: string] =
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

proc getRow*(
  self: DBMS,
  tbl: string,
  fieldsDesc: openArray[FieldDesc],
  stmt: string = "",
  params: varargs[string, `$`] = []): tuple[ok: bool, row: JsonNode, msg: string] =
  try:
    if not self.connected:
      return (false, nil, "can't connect to the database.")
    let select = fieldsDesc.map(proc (x: FieldDesc): string = x.name).join(",")
    let sqlStr = &"""SELECT {select} FROM"""
    let queryResult = self.conn.getRow(sql &"{sqlStr} {tbl} {stmt}", params)
    var res = %*{}
    if queryResult.len > 0 and queryResult[0] != "":
      for i in 0..fieldsDesc.high:
        for k, v in fieldsDesc[i].name.toDbType(fieldsDesc[i].nodeKind, queryResult[i]):
          var fname = k.toLower()
          if fname.contains(" as "):
            fname = fname.split(" as ")[1].strip
          res[fname] = v
    return (true, res, "ok")
  except Exception as ex:
    return (false, nil, ex.msg)

proc getAllRows*(
  self: DBMS,
  tbl: string,
  fieldsDesc: openArray[FieldDesc],
  stmt: string = "",
  params: varargs[string, `$`] = []): tuple[ok: bool, rows: seq[JsonNode], msg: string] =
  ###
  ### Retrieves a single row. If the query doesn't return any rows,
  ###
  try:
    if not self.connected:
      return (false, @[], "can't connect to the database.")
    let select = fieldsDesc.map(proc (x: FieldDesc): string = x.name).join(",")
    let sqlStr = &"""SELECT {select} FROM"""
    let queryResult = self.conn.getAllRows(sql &"{sqlStr} {tbl} {stmt}", params)
    var res: seq[JsonNode] = @[]
    if queryResult.len > 0 and queryResult[0][0] != "":
      for qres in queryResult:
        var resItem = %*{}
        for i in 0..fieldsDesc.high:
          for k, v in fieldsDesc[i].name.toDbType(fieldsDesc[i].nodeKind, qres[i]):
            var fname = k.toLower()
            if fname.contains(" as "):
              fname = fname.split(" as ")[1].strip
            resItem[fname] = v
        res.add(resItem)
    return (true, res, "ok")
  except Exception as ex:
    return (false, @[], ex.msg)

proc execAffectedRows*(
  self: DBMS,
  query: string,
  args: varargs[string, `$`] = []): tuple[ok: bool, affected: int64, msg: string] =
  ###
  ### runs the query (typically "UPDATE") and returns the number of affected rows
  ###
  try:
    if not self.connected:
      return (false, 0'i64, "can't connect to the database.")
    return (true, self.conn.execAffectedRows(sql query, args), "ok")
  except Exception as ex:
    return (false, 0'i64, ex.msg)

proc delete*(
  self: DBMS,
  tbl: string,
  stmt: string,
  params: varargs[string, `$`] = []): tuple[ok: bool, affected: int64, msg: string] =
  ###
  ### runs the query delete and returns the number of affected rows
  ###
  try:
    if not self.connected:
      return (false, 0'i64, "can't connect to the database.")
    var sqlStr = &"DELETE FROM"
    return (true, self.conn.execAffectedRows(sql &"{sqlStr} {tbl} {stmt}", params), "ok")
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

