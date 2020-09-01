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

        if c.success:
          result.conn = c.conn
        else:
          echo c.msg

      else:
        echo &"database {connId} not found!!."

    else:
      echo "database section not found!!."

# insert into database
proc insertId*(self: SqLite, tbl: string, keyValues: openArray[tuple[k: string, v: string]]): int64 =
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

  return self.conn.insertId(sql &"""{sqlStr} {tbl} ({keys.join(",")}) VALUES ({values.join(",")})""", valuesParam)

# try insert into database
proc tryInsertId*(self: SqLite, tbl: string, keyValues: openArray[tuple[k: string, v: string]]): int64 =
  ### try insert into the table
  ### return -1 if error occured
  result = -1
  try:
    result = self.insertId(tbl, keyValues)
  except:
    discard

proc update*(
  self: SqLite, tbl: string, keyValues: openArray[tuple[k: string, v: string]],
  where: string, whereParams: varargs[string, `$`]): int64 =
  ### update data table
  var sqlStr = "UPDATE"
  #var keys: seq[string] = @[]
  #var values: seq[string] = @[]
  #var valuesParam: seq[string] = @[]
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
  for wParams in whereParams:
    setValParam.add(wParams)

  return self.conn.execAffectedRows(sql &"""{sqlStr} {tbl} SET {setVal.join(",")} WHERE {where}""", setValParam)

proc tryUpdate*(self: SqLite, tbl: string, keyValues: openArray[tuple[k: string, v: string]],
  where: string, whereParams: varargs[string, `$`]): int64 =
  ### try update into the table
  ### return -1 if error occured
  result = -1
  try:
    result = self.update(tbl, keyValues, where, whereParams)
  except:
    discard

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

proc tryExec*(self: SqLite, query: SqlQuery, args: varargs[string, `$`]): bool =
  ###
  ### try execute the query return true if success
  ###
  return self.conn.tryExec(query, args)

proc exec*(self: SqLite, query: SqlQuery, args: varargs[string, `$`]) =
  ###
  ### execute the query raise dbError if error
  ###
  self.conn.exec(query, args)

proc getRow*(self: SqLite, query: SqlQuery, args: varargs[string, `$`]): Row =
  ###
  ### Retrieves a single row. If the query doesn't return any rows,
  ### this proc will return a Row with empty strings for each column
  ###
  return self.conn.getRow(query, args)

proc getAllRows*(self: SqLite, query: SqlQuery, args: varargs[string, `$`]): seq[Row] =
  ###
  ### executes the query and returns the whole result dataset
  ###
  return self.conn.getAllRows(query, args)

proc getValue*(self: SqLite, query: SqlQuery, args: varargs[string, `$`]): string =
  ###
  ### executes the query and returns the first column of the first row of the result dataset.
  ### Returns "" if the dataset contains no rows or the database value is NULL
  ###
  return self.conn.getValue(query, args)

proc execAffectedRows*(self: SqLite, query: SqlQuery, args: varargs[string, `$`]): int64 =
  ###
  ### runs the query (typically "UPDATE") and returns the number of affected rows
  ###
  return self.conn.execAffectedRows(query, args)

proc setEncoding(self: SqLite, encoding: string): bool =
  ###
  ### sets the encoding of a database connection, returns true for success, false for failure
  ###
  return self.conn.setEncoding(encoding)

template fastRows(self: SqLite, query: SqlQuery, args: varargs[string, `$`]): iterator =
  ###
  ### executes the query and iterates over the result dataset.
  ### This is very fast, but potentially dangerous. Use this iterator only if you require ALL the rows.
  same as fastRows, but slower and safe### Breaking the fastRows() iterator during a loop
  ### will cause the next database query to raise an [EDb] exception Commands out of sync.
  ###
  self.conn.fastRows(query, args)

template instantRows(self: SqLite, query: SqlQuery, args: varargs[string, `$`]): iterator =
  ###
  ### Same as fastRows but returns a handle that can be used to get column text on demand using [].
  ### Returned handle is valid only within the iterator body
  ###
  self.conn.instantRows(query, args)

template instantRows(self: SqLite, columns: var DbColumns, query: SqlQuery, args: varargs[string, `$`]): iterator =
  ###
  ### Same as fastRows but returns a handle that can be used to get column text on demand using [].
  ### Returned handle is valid only within the iterator body
  ###
  self.conn.instantRows(columns, query, args)

template rows(self: SqLite, query: SqlQuery, args: varargs[string, `$`]): iterator =
  ###
  ### same as fastRows, but slower and safe
  ###
  self.conn.rows(query, args)

proc getDbInfo*(self: SqLite): DbInfo =
  return self.dbInfo

# close the database connection
proc close*(self: SqLite) =
  try:
    self.conn.close
  except:
    discard

# test ping the server
proc ping*(self: SqLite): bool =
  try:
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
