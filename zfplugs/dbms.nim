##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

import strformat, times, macros, tables, typetraits, strutils, sequtils, json, options, re, db_mysql, db_postgres, db_sqlite
export options, strutils, sequtils, json, strformat

import stdext/[json_ext, strutils_ext, system_ext], dbs, settings, dbssql
export dbs, dbssql, json_ext, strutils_ext, system_ext

type
  DbInfo* = tuple[
    database: string,
    username: string,
    password: string,
    host: string, port: int]

  DBMS*[T] = ref object
    connId*: string
    dbInfo*: DbInfo
    conn*: T
    connected*: bool
    lastSql*: Sql

  KVObj* = tuple[
    keys: seq[string],
    values: seq[string],
    nodesKind: seq[JsonNodeKind]]

  InsertIdResult* = tuple[
    ok: bool,
    insertId: uint64,
    msg: string]

  UpdateResult* = tuple[
    ok: bool,
    affected: uint64,
    msg: string]
  
  ExecResult* = tuple[
    ok: bool,
    msg: string]

  RowResult*[T] = tuple[
    ok: bool,
    row: T,
    msg: string]
  
  CountResult* = tuple[
    ok: bool,
    count: uint64,
    msg: string]

  RowResults*[T] = tuple[
    ok: bool,
    rows: seq[T],
    msg: string]

  AffectedRowResults* = tuple[
    ok: bool,
    affected: int64,
    msg: string]

  DbmsDataType* = enum
    BIGINT
    INT
    SMALLINT
    DOUBLE
    FLOAT
    VARCHAR
    BOOL
    DATE
    TIME
    TIMESTAMP
    SERIAL
    TEXT

  DbmsType* = enum
    DBPGSQL
    DBMYSQL
    DBSQLITE

  DbmsModel* = ref object of RootObj

  DbmsStmtType* = enum
    SELECT
    INSERT
    DELETE
    UPDATE
    INNERJOIN
    LEFTJOIN
    RIGHTJOIN
    CREATE_TABLE
    COUNT

  DbmsFieldType* = ref object
    field: JFieldPair
    isPrimaryKey: bool
    isNull: bool
    foreignKeyRef: string
    name: string
    isUnique: bool
    length: uint64
    dataType: DbmsDataType
    foreignKeyOnUpdate: string
    foreignKeyOnDelete: string
    foreignKeyColumnRef: string
    tableName: string
    timeFormat: string
    dateFormat: string
    timestampFormat: string

##
##  dbmsTable pragma this is for type definition
##  will map to database table name
##
##  example:
##
##  type
##    Users
##      {.dbmsTable("users").} = ref object of DbmsModel
##      id
##        {.dbmsField(
##          isPrimaryKey = true,
##          dataType = SERIAL).}: Option[int]
##      name
##        {.dbmsField(
##          name = "full_name",
##          length = 255,
##          isNull = false).}: Option[string]
##      birthdate
##        {.dbmsField(
##          isNull = false,
##          dataType = TIMESTAMP).}: Option[string]
##      isOk {.ignoreField.}: Option[bool]
##      uid
##        {.dbmsField(
##          isNull = false,
##          isUnique = true,
##          length = 100).}: Option[string]
##  
##    Address
##      {.dbmsTable("address").} = ref object of DbmsModel
##      id
##        {.dbmsField(isPrimaryKey = true,
##          dataType = SERIAL,
##          isNull = false).}: Option[int]
##      address
##        {.dbmsField(length = 255).}: Option[string]
##      usersId
##        {.dbmsField("users_id",
##          dataType = BIGINT,
##          isNull = false)
##          dbmsForeignKeyRef: Users
##          dbmsForeignKeyColumnRef: Users.id.}: Option[int]
##
##

template dbmsTable*(name: string = "") {.pragma.}
template dbmsField*(
  name: string = "",
  isPrimaryKey: bool = false,
  isNull: bool = true,
  length: uint64 = 0,
  isUnique: bool = false,
  dataType: DbmsDataType = VARCHAR,
  timeFormat: string = "HH:mm:ss",
  dateFormat: string = "YYYY-MM-dd",
  timestampFormat: string = "YYYY-MM-dd HH:mm:ss") {.pragma.}
template dbmsForeignKeyRef*(foreignKeyRef: typed) {.pragma.}
template dbmsForeignKeyColumnRef*(foreignKeyColumnRef: typed) {.pragma.}
template dbmsForeignKeyConstraint*(
  onDelete: string = "CASCADE",
  onUpdate: string = "CASCADE",
  columnRef: string = "") {.pragma.}

##  var db: DBConn
##
##  this will read the settings.json on the section
##  "database": {
##    "your_connId_setting": {
##      "username": "",
##      "password": "",
##      "database": "",
##      "host": "",
##      "port": 1234
##    }
##  }
##
proc newDBMS*[T](connId: string): DBMS[T] {.gcsafe.} =
  ##
  ##  create new DBMS object (database connection)
  ##  pass connId from settings.json in database section
  ##  let myconn = newDBMS[MySql]("zcms")
  ##
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

proc newDBMS*[T](
  database: string,
  username: string,
  password: string,
  host: string,
  port: int): DBMS[T] {.gcsafe.} =
  ##
  ##  Create newDBMS for database connection with given parameter
  ##  let myConn = newDBMS[MySql](
  ##    "test", "admin",
  ##    "123321", "localhost",
  ##    3306)
  ##
  let c = newDbs[T](
    database,
    username,
    password,
    host,
    port).tryConnect()

  result = DBMS[T](connId: "")
  result.connected = c.success
  if c.success:
    result.conn = c.conn
  else:
    echo c.msg

proc tryConnect*[T](self: DBMS[T]): bool {.gcsafe.} =
  ##
  ## Try connect to database
  ## Generic T is type of MySql, PgSql, SqLite
  ##

  let c = newDbs[T](
    self.dbInfo.database,
    self.dbInfo.username,
    self.dbInfo.password,
    self.dbInfo.host,
    self.dbInfo.port).tryConnect()
  self.conn = c.conn
  self.connected = c.success
  result = self.connected

proc dbmsQuote*(str: string): string =
  ##
  ##  quote special char from the string make it valid for sql
  ##
  result = (fmt"{str}")
    .replace(fmt"\", fmt"\\")
    .replace(fmt"'", fmt"\'")
    .replace(fmt""" " """.strip, fmt""" \" """.strip)
    .replace(fmt"\x1a", fmt"\\Z")

proc extractKeyValue*[T](
  self: DBMS,
  obj: T): KVObj {.gcsafe.} =
  ##
  ##  Extract key and value og the type
  ##  will discard null value
  ##  let kv = users.extractKeyValue
  ##
  ##  will retur KVObj
  ##
  ##  KVObj* = tuple[
  ##    keys: seq[string],
  ##    values: seq[string],
  ##    nodesKind: seq[JsonNodeKind]]
  ##
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

  result = (keys, values, nodesKind)

proc dbmsQuote*(q: Sql): string =
  ##
  ## Quote Sql and return string, will quote special char to be valid sql
  ##
  let q = q.toQs
  var queries = q.query.split("?")
  for i in 0..q.params.high:
    let p = q.params[i]
    #var v = if p.nodeKind == JString and p.val != "null":
    #var v = if p.kind != JNull:
    #    #&"'{dbmsQuote(p.val)}'"
    #    &"'{dbmsQuote(v)}'"
    #  else:
    #    #p.val
    #    "null"
    var v = if p.kind == JString:
        &"'{dbmsQuote(p.getStr)}'"
      elif p.kind == JNull:
        "null"
      else:
        $p

    queries.insert([v], (i*2) + 1)

  result = queries.join("")

# insert into database
proc insertId*[T](
  self: DBMS,
  table: string,
  obj: T): InsertIdResult {.gcsafe.} =
  ##
  ##  insert into database and return as InsertIdResult
  ##
  ##  let insert = db.insertId("users", Users(name: "Jhon"))
  ##  if insert.ok:
  ##    echo insert.insertId
  ##  echo insert.msg
  ##
  var q = Sql()
  try:
    if not self.connected:
      result = (false, 0'i64, "can't connect to the database.")
    else:
      let kv = self.extractKeyValue(obj)
      #var fieldItems: seq[JFieldItem] = @[]
      var fieldItems: seq[JsonNode] = @[]
      for i in 0..kv.keys.high:
        #fieldItems.add((kv.values[i], kv.nodesKind[i]))
        fieldItems.add(%i)

      q = Sql()
        .insert(table, kv.keys)
        .value(fieldItems)
      
      result = (true,
        self.conn.insertId(sql dbmsQuote(q)),
        "ok")

  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo dbmsQuote(q)
    result = (false, 0'i64, ex.msg)

proc update*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): UpdateResult {.gcsafe.} =
  ##
  ##  update table will return UpdateResult
  ##
  ##  let update = db.update("users", Users(id: 100, name: "Jhon Doe"))
  ##  if update.ok:
  ##    echo update.affected
  ##  echo update.msg
  ##
  var q = Sql()
  try:
    if not self.connected:
      result = (false, 0'i64, "can't connect to the database.")
    else:
      let kv = self.extractKeyValue(obj)
      #var fieldItems: seq[JFieldItem] = @[]
      var fieldItems: seq[JsonNode] = @[]
      for i in 0..kv.keys.high:
        #fieldItems.add((kv.values[i], kv.nodesKind[i]))
        fieldItems.add(%i)

      q = Sql()
        .update(table, kv.keys)
        .value(fieldItems) & query
      
      result = (true,
        self.conn.execAffectedRows(sql dbmsQuote(q)),
        "ok")

  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo dbmsQuote(q)
    result = (false, 0'i64, ex.msg)

proc exec*(
  self: DBMS,
  query: Sql): ExecResult {.gcsafe.} =
  ##
  ##  execute the query will return ExecResult
  ##
  ##  db.exec(Sql().delete("users").where("users.id=?", %100))
  ##
  var q = Sql()
  try:
    if not self.connected:
      result = (false, "can't connect to the database.")
    else:
      q = query
      
      self.conn.exec(sql dbmsQuote(q))
      result = (true, "ok")

  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo dbmsQuote(q)
    result = (false, ex.msg)
    
proc extractQueryResults*(fields: seq[JFieldDesc], queryResults: seq[string]): JsonNode {.gcsafe.} =
  result = %*{}
  if queryResults.len > 0 and queryResults[0] != "" and queryResults.len == fields.len:
    for i in 0..fields.high:
      for k, v in fields[i].name.toDbType(fields[i].nodeKind, queryResults[i]):
        var fprops = k.split(" AS ")
        result[fprops[fprops.high].strip] = v

proc getCount*(
  self: DBMS,
  query: Sql): CountResult {.gcsafe.} =
  ##
  ##  get count result return CountResult
  ##
  ##  let count = db.getCount(Sql().select("count(users.id)")
  ##    .fromTable("users").where("is_active=?", %true))
  ##  if count.ok:
  ##    echo count.count
  ##  echo msg
  ##
  try:
    if not self.connected:
      result = (false, 0'i64, "can't connect to the database.")
    else:
      let queryResults = self.conn.getRow(sql dbmsQuote(query))
      let countResult = tryParseBiggestUInt(queryResults[0])
      result = (countResult.ok, countResult.val, "ok")
  except Exception as ex:
    echo &"{ex.msg}, {query.toQs}"
    echo dbmsQuote(query)
    result = (false, 0'i64, ex.msg)

proc getRow*[T](
  self: DBMS,
  obj: T,
  query: Sql): RowResult[T] {.gcsafe.} =
  ##
  ##  get row from database will return RowResult
  ##
  ##  let r = db.getRow(Users(), Sql().where("users.id=?", %100))
  ##  if r.ok:
  ##    echo %r.row
  ##  echo r.msg
  ##
  try:
    if not self.connected:
      result = (false, obj, "can't connect to the database.")
    else:
      let fields = obj.fieldDesc
      let queryResults = self.conn.getRow(sql dbmsQuote(query))
      result = (true, extractQueryResults(fields, queryResults).to(T), "ok")
  except Exception as ex:
    echo &"{ex.msg}, {query.toQs}"
    echo dbmsQuote(query)
    result = (false, obj, ex.msg)

proc getRow*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): RowResult[T] {.gcsafe.} =
  ##
  ##  get row result from database and return RowResult
  ##
  ##  let r = db.getRow("users", Users(), Sql().where("users.id=?", %100))
  ##  if r.ok:
  ##    echo %r.row
  ##  echo r.msg
  ##
  var q = Sql()
  try:
    if not self.connected:
      result = (false, obj, "can't connect to the database.")
    else:
      let fields = obj.fieldsDesc
      q = (Sql()
        .select(fields.map(proc(x: JFieldDesc): string = x.name))
        .fromTable(table) & query)
       
      let queryResults = self.conn.getRow(sql dbmsQuote(q))
      result = (true, extractQueryResults(fields, queryResults).to(T), "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo dbmsQuote(q)
    result = (false, obj, ex.msg)

proc getRows*[T](
  self: DBMS,
  obj: T,
  query: Sql): RowResults[T] {.gcsafe.} =
  ##
  ##  get multiple rows from database will return RowResults
  ##
  ##  let r = db.getRows(Users(), Sql().where("users.is_active=?", %true))
  ##  if r.ok:
  ##    echo %r.rows
  ##  echo r.msg
  ##
  try:
    if not self.connected:
      result = (false, @[], "can't connect to the database.")
    else:
      let fields = obj.fieldDesc
      let queryResults = self.conn.getAllRows(sql dbmsQuote(query))
      var res: seq[T] = @[]
      if queryResults.len > 0 and queryResults[0][0] != "":
        for qres in queryResults:
          res.add(extractQueryResults(fields, qres).to(T))
      result = (true, res, "ok")
  except Exception as ex:
    echo &"{ex.msg}, {query.toQs}"
    echo dbmsQuote(query)
    result = (false, @[], ex.msg)

proc getRows*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): RowResults[T] {.gcsafe.} =
  ##
  ##  get multiple rows from database will return RowResults
  ##
  ##  let r = db.getRows("users", Users(), Sql().where("users.is_active=?", %true))
  ##  if r.ok:
  ##    echo %r.rows
  ##  echo r.msg
  ##
  var q = Sql()
  try:
    if not self.connected:
      result = (false, @[], "can't connect to the database.")
    else:
      let fields = obj.fieldsDesc
      q = (Sql()
        .select(fields.map(proc(x: JFieldDesc): string = x.name))
        .fromTable(table) & query)

      let queryResults = self.conn.getRows(sql dbmsQuote(q))
      var res: seq[T] = @[]
      if queryResults.len > 0 and queryResults[0][0] != "":
        for qres in queryResults:
          res.add(extractQueryResults(fields, qres).to(T))
      result = (true, res, "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo dbmsQuote(q)
    result = (false, @[], ex.msg)

proc execAffectedRows*(
  self: DBMS,
  query: Sql): AffectedRowResults {.gcsafe.} =
  ##
  ##  exec query and get affected row will return AffectedRowResults
  ##  let r = db.execAffectedRows(Sql().delete("users").where("users.id=?", %100))
  ##  if r.ok:
  ##    echo r.affected
  ##  echo r.msg
  ##
  var q = Sql()
  try:
    if not self.connected:
      result = (false, 0'i64, "can't connect to the database.")
    else:
      q = query

      result = (true, self.conn.execAffectedRows(sql dbmsQuote(q)), "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo dbmsQuote(q)
    result = (false, 0'i64, ex.msg)

proc delete*[T](
  self: DBMS,
  table: string,
  obj: T,
  query: Sql): AffectedRowResults {.gcsafe.} =
  ##
  ##  exec delete query and get affected row will return AffectedRowResults
  ##  let r = db.delete("users", Users(id: 100))
  ##  if r.ok:
  ##    echo r.affected
  ##  echo r.msg
  ##
  var q = Sql()
  try:
    if not self.connected:
      result = (false, 0'i64, "can't connect to the database.")
    else:
      q = (Sql()
        .delete(table) & query)
      
      result = (true, self.conn.execAffectedRows(sql dbmsQuote(q)), "ok")
  except Exception as ex:
    echo &"{ex.msg}, {q.toQs}"
    echo dbmsQuote(q)
    result = (false, 0'i64, ex.msg)

proc setEncoding(
  self: DBMS,
  encoding: string): bool {.gcsafe.} =
  ##
  ## sets the encoding of a database connection, returns true for success, false for failure
  ##
  if not self.connected:
    result = false
  else:
    result = self.conn.setEncoding(encoding)

proc getDbInfo*(self: DBMS): DbInfo {.gcsafe.} =
  ##
  ##  get database info
  ##
  result = self.dbInfo

# close the database connection
proc close*(self: DBMS) {.gcsafe.} =
  ##
  ##  close database connection
  ##
  try:
    self.conn.close
  except:
    discard
  self.connected = false

# test ping the server
proc ping*(self: DBMS): bool {.gcsafe.} =
  ##
  ## ping to checn the database connection instance
  ## return true if connection active
  ##
  try:
    if not self.connected:
      result = self.connected
    else:
      discard self.conn.getRow(sql "SELECT 1")
      result = true
  except Exception as e:
    echo e.msg
    self.close
    discard

# get connId
proc connId*(self: DBMS): string {.gcsafe.} =
  ##
  ##  get database connId
  ##
  if not self.isNil:
    result = self.connId

proc startTransaction*(self: DBMS): ExecResult {.gcsafe discardable.} =
  ##
  ##  start database transaction
  ##
  result = self.exec(Sql().startTransaction)

proc commitTransaction*(self: DBMS): ExecResult {.gcsafe discardable.} =
  ##
  ##  commit database transaction
  ##
  result = self.exec(Sql().commitTransaction)

proc savePointTransaction*(
  self: DBMS,
  savePoint: string): ExecResult {.gcsafe discardable.} =
  ##
  ##  create save point of transaction
  ##
  result = self.exec(Sql().savePointTransaction(savePoint))

proc rollbackTransaction*(
  self: DBMS,
  savePoint: string = ""): ExecResult {.gcsafe discardable.} =
  ##
  ##  rollback transaction
  ##
  result = self.exec(Sql().rollbackTransaction(savePoint))
  if result.ok:
    result = self.exec(Sql().commitTransaction)

proc toWhereQuery*[T](
  obj: T,
  tablePrefix: string = "",
  op: string = "AND"): tuple[where: string, params: seq[JFieldItem]] =
  ##
  ##  convert object to qeuery syntanx default is AND operator
  ##
  result = (%obj).toWhereQuery(tablePrefix, op)

proc getDbType(dbms: Dbms): DbmsType =
  ##
  ##  get database type
  ##
  if dbms.conn is PgSql:
    result = DBPGSQL
  elif dbms.conn is MySql:
    result = DBMYSQL
  else:
    result = DBSQLITE

proc generateCreateTable(
  dbmsType: DbmsType,
  fieldList: seq[DbmsFieldType]): Sql =
  ##
  ##  create table syntax generator depend on database type
  ##
  var columns: seq[string] = @[]
  var primaryKey: seq[string] = @[]
  var foreignKey: seq[string] = @[]
  var tableName: string
  
  for f in fieldList:
    var column: seq[string] = @[]
    var columnName: string = ""

    if tableName == "":
      tableName = f.tableName

    if f.field.name != "":
      columnName = f.field.name
    else:
      columnName = f.name

    column.add(columnName)
    if f.isPrimaryKey:
      primaryKey.add(columnName)

    var isAutoInc = false
    case f.dataType
    of BIGINT:
      column.add("BIGINT")
    of INT:
      column.add("INT")
    of SMALLINT:
      column.add("SMALLINT")
    of DOUBLE:
      if dbmsType != DBPGSQL:
        column.add("DOUBLE")
      else:
        column.add("DOUBLE PRECISION")
    of FLOAT:
      if dbmsType != DBPGSQL:
        column.add("FLOAT")
      else:
        column.add("REAL")
    of VARCHAR:
      column.add("VARCHAR")
    of BOOL:
      column.add("BOOL")
    of TIME:
      column.add("TIME")
    of DATE:
      column.add("DATE")
    of TIMESTAMP:
      if dbmsType != DBPGSQL:
        column.add("DATETIME")
      else:
        column.add("TIMESTAMPTZ")
    of SERIAL:
      if dbmsType == DBMYSQL or dbmsType == DBSQLITE:
        # Mysql SqLite
        isAutoInc = true
        column.add("BIGINT")
      else:
        # PgSql SERIAL
        column.add("SERIAL")
    of TEXT:
      if dbmsType != DBPGSQL:
        column.add("LONGTEXT")
      else:
        column.add("TEXT")

    if f.length > 0:
      column.add(&"({f.length})")

    if dbmsType != DBPGSQL:
      if isAutoInc:
        if dbmsType == DBMYSQL:
          column.add("AUTO_INCREMENT")
        else:
          column.add("AUTOINCREMENT")

    if f.dataType != SERIAL and not f.isPrimaryKey:
      if f.isNull:
        column.add("NULL")
      else:
        column.add("NOT NULL")

      if f.isUnique:
        column.add("UNIQUE")

    if f.foreignKeyRef != "":
      var fkColRef = f.foreignKeyColumnRef
      if fkColRef == "":
        fkColRef = "id"

      var onUpdate = ""
      if f.foreignKeyOnUpdate != "":
        onUpdate = &"ON UPDATE {f.foreignKeyOnUpdate}"
      
      var onDelete = ""
      if f.foreignKeyOnDelete != "":
        onDelete = &"ON DELETE {f.foreignKeyOnDelete}"

      foreignKey.add(&"""FOREIGN KEY ({columnName}) REFERENCES {f.foreignKeyRef}({fkColRef}) {onUpdate} {onDelete}""")

    columns.add(column.join(" "))

  if primaryKey.len != 0:
    columns.add(&"""PRIMARY KEY({primaryKey.join(", ")})""")

  if foreignKey.len != 0:
    columns &= foreignKey
  
  result = Sql()
  result.stmt.add(&"""CREATE TABLE {tableName}({columns.join(", ")})""")

proc generateSelectTable(
  fieldList: seq[DbmsFieldType],
  query: Sql = Sql()): Sql =
  ##
  ##  select table syntax generator
  ##
  let q = Sql()
  var tableName = ""
  var fields: seq[string] = @[]
  var where = Sql()

  for f in fieldList:
    if tableName == "":
      tableName = f.tableName

    var fieldName = f.name
    if f.field.name != "":
      fieldName = f.field.name
    fields.add(fieldName)
    
    if f.field.val != "null":
      if where.stmt.len == 0:
        discard where.where(&"{tableName}.{fieldName}=?", %f.field.val)
      else:
        discard where.andWhere(&"{tableName}.{fieldName}=?", %f.field.val)

  result = q.select(fields).fromTable(tableName) & where & query

proc generateCountTable(
  fieldList: seq[DbmsFieldType],
  query: Sql = Sql()): Sql =
  ##
  ##  count table syntax generator
  ##
  let q = Sql()
  var tableName = ""
  var where = Sql()

  for f in fieldList:
    if tableName == "":
      tableName = f.tableName

    var fieldName = f.name
    if f.field.name != "":
      fieldName = f.field.name
    
    if f.field.val != "null":
      if where.stmt.len == 0:
        discard where.where(&"{tableName}.{fieldName}=?", %f.field.val)
      else:
        discard where.andWhere(&"{tableName}.{fieldName}=?", %f.field.val)

  result = q.select("COUNT(*)", withTablePrefix = false).fromTable(tableName) & where & query

proc generateInsertTable(
  multiFieldList: seq[seq[DbmsFieldType]]): Sql =
  ##
  ##  insert table syntax generator
  ##
  let q = Sql()
  # prepare of multiple insert
  # if single insert then get the first index
  #var multiValues: seq[seq[JFieldItem]] = @[]
  var multiValues: seq[JsonNode] = @[]
  var fields: seq[string] = @[]
  var tableName = ""
  var isExtractFieldComplete = false

  for multiField in multiFieldList:
    #var values: seq[JFieldItem] = @[]
    var values: seq[JsonNode] = @[]

    for f in multiField:
      # only set table name if empty
      if tableName == "":
        tableName = f.tableName

      var fieldName = f.name
      if f.field.name != "":
        fieldName = f.field.name

      if f.field.val != "null":
        if not isExtractFieldComplete:
          fields.add(fieldName)

        values.add(%f.field.val)

    isExtractFieldComplete = true
    multiValues.add(values)

  if multiValues.len == 0:
    result = q.insert(tableName, fields).value(multiValues[0])
  else:
    result = q.insert(tableName, fields).values(multiValues)

proc generateUpdateTable(
  fieldList: seq[DbmsFieldType],
  query: Sql = Sql()): Sql =
  ##
  ##  update table syntax generator
  ##
  let q = Sql()
  var tableName = ""
  #var value: seq[JFieldItem] = @[]
  var value: seq[JsonNode] = @[]
  let where = Sql()
  var fields: seq[string] = @[]

  for f in fieldList:
    if tableName == "":
      tableName = f.tableName

    var fieldName = f.name
    if f.field.name != "":
      fieldName = f.field.name
    fields.add(fieldName)
        
    value.add(%f.field.val)
    
    if f.isPrimaryKey:
      if where.stmt.len == 0:
        discard where.where(&"{tableName}.{fieldName}=?", %f.field.val)
      else:
        discard where.andWhere(&"{tableName}.{fieldName}=?", %f.field.val)

  result = q.update(tableName, fields).value(value) & where & query

proc generateDeleteTable(
  multiFieldList: seq[seq[DbmsFieldType]],
  query: Sql = Sql()): Sql =
  ##
  ##  delete table syntax generator
  ##
  let q = Sql()
  var tableName = ""
  let where = Sql()

  for multiField in multiFieldList:
    let fieldFilter = Sql()
    for f in multiField:
      if tableName == "":
        tableName = f.tableName

      var fieldName = f.name
      if f.field.name != "":
        fieldName = f.field.name

      if f.field.val != "null":
        if fieldFilter.stmt.len == 0:
          discard fieldFilter.append(&"{tableName}.{fieldName}=?", %f.field.val)
        else:
          discard fieldFilter.append(&"AND {tableName}.{fieldName}=?", %f.field.val)

    if where.stmt.len == 0:
      discard where.bracket(fieldFilter)
    else:
      discard where.append("OR").bracket(fieldFilter)

  result = q.delete(tableName).where(where) & query

proc generateJoinTable(
  fieldListTbl1: seq[DbmsFieldType],
  fieldListTbl2: seq[DbmsFieldType],
  stmtType: DbmsStmtType): Sql =
  ##
  ##  join table syntax generator
  ##
  let tableName: array[2, string] = [
    fieldListTbl1[0].tableName,
    fieldListTbl2[0].tableName]
  var fields: seq[string] = @[]
  var joinPair: seq[string] = @[]
  var joinType = ""
  let q = Sql()
 
  # get first tablename
  for f in fieldListTbl1:
    var fieldName = f.name
    if f.field.name != "":
      fieldName = f.field.name
    fields.add(&"{f.tableName}.{fieldName}")

    echo "---"
    echo f.foreignKeyColumnRef
    echo f.foreignKeyRef
    echo "---"
    if f.foreignKeyColumnRef != "" and
      f.foreignKeyRef in tableName:
      joinPair.add(&"{f.tableName}.{fieldName}={f.foreignKeyRef}.{f.foreignKeyColumnRef}")

  # get second tablename
  for f in fieldListTbl2:
    var fieldName = f.name
    if f.field.name != "":
      fieldName = f.field.name
    fields.add(&"{f.tableName}.{fieldName}")

    if f.foreignKeyColumnRef != "" and
      f.foreignKeyRef in tableName:
      joinPair.add(&"{f.tableName}.{fieldName}={f.foreignKeyRef}.{f.foreignKeyColumnRef}")
    

  #discard q.select(fields, false).fromTable(tableName[0])
  
  case stmtType
  of INNERJOIN:
    discard q.innerJoin(tableName[1], joinPair)
  of LEFTJOIN:
    discard q.leftJoin(tableName[1], joinPair)
  of RIGHTJOIN:
    discard q.rightJoin(tableName[1], joinPair)
  else:
    discard

  result = q

proc validatePragma[T](t: T): seq[DbmsFieldType] =
  ##
  ##  validation check pragma syntax, check pragma definition of the type
  ##
  when t.hasCustomPragma(dbmsTable):
    var fieldList: seq[DbmsFieldType] = @[]
    var dbmsTablePragma = t.getCustomPragmaVal(dbmsTable)
    if dbmsTablePragma == "":
      dbmsTablePragma = ($typeof(t)).split(":")[0]
    if dbmsTablePragma.strip != "":
      for k, v in system.fieldPairs(t):
        when v.hasCustomPragma(dbmsField):
          let dbmsFieldType = DbmsFieldType()
          dbmsFieldType.name = k
          dbmsFieldType.tableName = dbmsTablePragma
          
          let dbmsFieldPragma = v.getCustomPragmaVal(dbmsField)
          dbmsFieldType.isNull = dbmsFieldPragma.isNull
          dbmsFieldType.isPrimaryKey = dbmsFieldPragma.isPrimaryKey
          dbmsFieldType.isUnique = dbmsFieldPragma.isUnique
          dbmsFieldType.length = dbmsFieldPragma.length.uint64
          dbmsFieldType.dataType = dbmsFieldPragma.dataType
          dbmsFieldType.timeFormat = dbmsFieldPragma.timeFormat
          dbmsFieldType.dateFormat = dbmsFieldPragma.dateFormat
          dbmsFieldType.timestampFormat = dbmsFieldPragma.timestampFormat
          
          var (name, val, nodeKind) = dbmsFieldPragma.name.fieldPair(v)
          if val != "null":
            if dbmsFieldType.dataType in [TIMESTAMP, TIME, DATE]:
              if val.contains("T"):
                let dt = val.parse("yyyy-MM-dd'T'HH:mm:sszzz")

                case dbmsFieldType.dataType
                of TIMESTAMP:
                  val = dt.format(dbmsFieldType.timestampFormat)
                of TIME:
                  val = dt.format(dbmsFieldType.timeFormat)
                of DATE:
                  val = dt.format(dbmsFieldType.dateFormat)
                else:
                  discard

          dbmsFieldType.field = (name, val, nodeKind)

          when v.hasCustomPragma(dbmsForeignKeyRef):
            dbmsFieldType.foreignKeyRef = v.getCustomPragmaVal(dbmsForeignKeyRef).getCustomPragmaVal(dbmsTable)
            if dbmsFieldType.foreignKeyRef == "":
              dbmsFieldType.foreignKeyRef = ($(typeof v.getCustomPragmaVal(dbmsForeignKeyRef))).split(":")[0]
          
          when v.hasCustomPragma(dbmsForeignKeyColumnRef):
            dbmsFieldType.foreignKeyColumnRef = ($>v.getCustomPragmaVal(dbmsForeignKeyColumnRef)).replace(re"^(.+?)\.", "")
          
          when v.hasCustomPragma(dbmsForeignKeyConstraint):
            let pragmaConstraint = v.getCustomPragmaVal(dbmsForeignKeyConstraint)
            dbmsFieldType.foreignKeyOnUpdate = pragmaConstraint.onUpdate
            dbmsFieldType.foreignKeyOnDelete = pragmaConstraint.onDelete
            dbmsFieldType.foreignkeyColumnRef = pragmaConstraint.columnRef

          fieldList.add(dbmsFieldType)

    result = fieldList
  
  else:
    raise newException(ObjectConversionDefect, "object definition not contain pragma {.dbmsTable(table_name).}.")

proc stmtTranslator[T1, T2](
  tbl1: T1,
  tbl2: T2,
  stmtType: DbmsStmtType): Sql =
  ##
  ## join statement generator
  ##
  var fieldListTbl1: seq[DbmsFieldType] = @[]
  var fieldListTbl2: seq[DbmsFieldType] = @[]

  when tbl1 is object:
    fieldListTbl1 = tbl1.validatePragma
  else:
    fieldListTbl1 = tbl1[].validatePragma

  when tbl2 is object:
    fieldListTbl2 = tbl2.validatePragma
  else:
    fieldListTbl2 = tbl2[].validatePragma

  if fieldListTbl1.len != 0 and fieldListTbl2.len != 0:
    result = generateJoinTable(fieldListTbl1, fieldListTbl2, stmtType)

proc stmtTranslator[T](
  dbmsType: DbmsType,
  t: T,
  stmtType: DbmsStmtType,
  query: Sql = Sql()): Sql =
  ##
  ##  sql sstatement translator
  ##
  var multiFieldList: seq[seq[DbmsFieldType]] = @[]
  when t is seq or t is array:
    # for multiple fieldlist, like insert, update, join
    for i in t:
      when i is object:
        multiFieldList.add(i.validatePragma)
      elif i is ref object:
        multiFieldList.add(i[].validatePragma)
  else:
    when t is object:
      multiFieldList.add(t.validatePragma)
    elif t is ref object:
      multiFieldList.add(t[].validatePragma)

  if multiFieldList.len != 0:
    case stmtType
    of SELECT:
      result = generateSelectTable(multiFieldList[0], query)
    of INSERT:
      result = generateInsertTable(multiFieldList)
    of UPDATE:
      result = generateUpdateTable(multiFieldList[0], query)
    of DELETE:
      result = generateDeleteTable(multiFieldList, query)
    of CREATE_TABLE:
      result = dbmsType.generateCreateTable(multiFieldList[0])
    of COUNT:
      result = generateCountTable(multiFieldList[0])
    else:
      discard

proc createTable*[T](
  dbms: Dbms,
  t: T): ExecResult  =
  ##
  ##  create new table with given object
  ##
  ##  let ctbl = db.createTable(Users())
  ##  if ctbl.ok:
  ##    echo "table created"
  ##  echo ctbl.msg
  ##
  result = dbms.exec(dbms.getDbType.stmtTranslator(t, CREATE_TABLE))

proc select*[T](
  dbms: Dbms,
  t: T,
  query: Sql = Sql()): RowResults[T] =
  ##
  ##  select multiple rows result from table with given object
  ##
  ##  let r = db.select(Users(isActive: some true))
  ##  if r.ok:
  ##    echo r.rows
  ##  echo r.msg
  ##
  if query.stmt.len == 0:
    discard query.limit(30)
  
  result = dbms.getRows(t, dbms.getDbType.stmtTranslator(t, SELECT, query))

proc selectOne*[T](
  dbms: Dbms,
  t: T,
  query: Sql = Sql()): RowResult[T] =
  ##
  ##  select single row result from table with given object
  ##
  ##  let r = db.select(Users(id: some 100))
  ##  if r.ok:
  ##    echo r.row
  ##  echo r.msg
  ##
  result = dbms.getRow(t, dbms.getDbType.stmtTranslator(t, SELECT, query))

proc innerJoin*[T1, T2](
  tbl1: T1,
  tbl2: T2): Sql =
  ##
  ##  inner join two object will return Sql object
  ##
  ##  let r = db.select(Users(isActive: some true),
  ##    Users().innerJoin(Address()) &
  ##    Address().leftJoin(DetailsAddress()))
  ##
  result = stmtTranslator(tbl1, tbl2, INNERJOIN)

proc leftJoin*[T1, T2](
  tbl1: T1,
  tbl2: T2): Sql =
  ##
  ##  inner join two object will return Sql object
  ##
  ##  let r = db.select(Users(isActive: some true),
  ##    Users().innerJoin(Address()) &
  ##    Address().leftJoin(DetailsAddress()))
  ##
  result = stmtTranslator(tbl1, tbl2, LEFTJOIN)

proc rightJoin*[T1, T2](
  tbl1: T1,
  tbl2: T2): Sql =
  ##
  ##  inner join two object will return Sql object
  ##
  ##  let r = db.select(Users(isActive: some true),
  ##    Users().innerJoin(Address()) &
  ##    Address().rightJoin(DetailsAddress()))
  ##
  result = stmtTranslator(tbl1, tbl2, RIGHTJOIN)

proc count*[T](
  dbms: Dbms,
  t: T,
  query: Sql = Sql()): CountResult =
  ##
  ##  count row of given object
  ##
  ##  let count = db.count(Users(isActive: some true))
  ##  if count.ok:
  ##    echo count.count
  ##  echo count.msg
  ##
  result =  dbms.getCount(dbms.getDbType.stmtTranslator(t, COUNT, query))

proc insert*[T](
  dbms: Dbms,
  t: T): AffectedRowResults =
  ##
  ##  insert to table with given object or list of object
  ##
  ##  let r = db.insert(Users(name: some "Jhon Doe"))
  ##  let mr = db.insert(
  ##    Users(name: some "Jhon Doe"),
  ##    Users(name: some "Michel Foe"))
  ##
  result = dbms.execAffectedRows(dbms.getDbType.stmtTranslator(t, INSERT))

proc update*[T](
  dbms: Dbms,
  t: T,
  query: Sql = Sql()): AffectedRowResults =
  ##
  ##  update table with given object or list object
  ##
  ##  let r = db.update(Users(id: some 100, name: some "Jhon Chena"),
  ##    Users(id: some 200, name: some "Michel Bar"))
  ##
  var affectedRows: int64 = 0'i64
  var ok: bool
  var msg: string = "failed"

  let startTransaction = dbms.exec(Sql().startTransaction)
  ok = startTransaction.ok
  msg = startTransaction.msg

  if startTransaction.ok:
    when t is array or t is seq:
      for it in t:
        let selectIt = dbms.select(it)
        if selectIt.ok:
          if dbms.execAffectedRows(dbms.getDbType.stmtTranslator(selectIt.row.patch(it), UPDATE, query)).ok:
            affectedRows += 1
    else:
      let selectT = dbms.select(t)
      if selectT.ok:
        if dbms.execAffectedRows(dbms.getDbType.stmtTranslator(selectT.row.patch(t), UPDATE, query)).ok:
          affectedRows = 1
          msg = "ok"

  let commitTransaction = dbms.exec(Sql().commitTransaction)
  ok = commitTransaction.ok
  msg = commitTransaction.msg
  result = (ok, affectedRows, msg)

proc delete*[T](
  dbms: Dbms,
  t: T,
  query: Sql = Sql()): AffectedRowResults =
  ##
  ##  delete from table with given obaject
  ##
  ##  let r = db.delete(Users(id: some 100))
  ##
  result = dbms.execAffectedRows(dbms.getDbType.stmtTranslator(t, DELETE, query))

