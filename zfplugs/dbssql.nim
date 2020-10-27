import db_common, sequtils, strformat, strutils, json
import stdext/[strutils_ext, json_ext]

type
  Sql* = ref object
    fields: seq[string]
    stmt: seq[string]
    params: seq[FieldItem]

#proc toQ*(self: Sql): tuple[fields: seq[string], query: SqlQuery, params: seq[string]] =
proc toQ*(self: Sql): tuple[fields: seq[string], query: SqlQuery, params: seq[FieldItem]] =
  
  #return (self.fields, sql self.stmt.join(" "), self.params)
  return (self.fields, sql self.stmt.join(" "), self.params)

#proc toQs*(self: Sql): tuple[fields: seq[string], query: string, params: seq[string]] =
proc toQs*(self: Sql): tuple[fields: seq[string], query: string, params: seq[FieldItem]] =
  
  #return (self.fields, self.stmt.join(" "), self.params)
  return (self.fields, self.stmt.join(" "), self.params)

proc `$`*(self: Sql): string =
  return $self.toQs

proc `&`*(self: Sql, other: Sql): Sql =
  if other.stmt.len != 0:
    self.stmt &= other.stmt
    self.params &= other.params
  
  return self

proc extractFields(
  self: Sql,
  fields: openArray[string]): seq[string] =

  return fields.map(proc (x: string): string =
    let field = x.toLower.split(" as ")
    result = field[field.high])

#proc sqlTransaction*(sqls: varargs[Sql]): tuple[query: string, params: seq[string]] =
proc sqlTransaction*(sqls: varargs[Sql]): tuple[query: string, params: seq[FieldItem]] =
  #var sqlParams: seq[string] = @[]
  var sqlParams: seq[FieldItem] = @[]
  return ((
    @["BEGIN"] &
    sqls.map(proc (x: Sql): string =
      let q = x.toQs
      sqlParams &= q.params
      return q.query) &
    @["COMMIT;"]).join(";"),
    sqlParams)

proc dropDatabase*(
  self: Sql,
  database: string): Sql =

  self.stmt.add(&"DROP DATABASE {database}")
  return self

proc dropTable*(
  self: Sql,
  table: string): Sql =

  self.stmt.add(&"DROP TABLE {table}")
  return self

proc truncateTable*(
  self: Sql,
  table: string): Sql =

  self.stmt.add(&"TRUNCATE TABLE {table}")
  return self

#### query generator helper
proc select*(
  self: Sql,
  fields: varargs[string, `$`]): Sql =
  
  self.fields &= self.extractFields(fields)
  let mapFields = fields.map(proc (x: string): string =
    result = x
    if not x.toLower.contains(" as "):
      result = &"{{table}}.{x}")
  
  self.stmt.add(&"""SELECT {mapFields.join(", ")}""")

  return self

proc select*(
  self: Sql,
  fields: openArray[string],
  fieldsQuery: openArray[tuple[query: Sql, fieldAlias: string]]): Sql =
  
  var fieldsList: seq[string]
  if fields.len > 0:
    fieldsList = fields.map(proc (x: string): string =
      result = x
      if not x.toLower.contains(" as "):
        result = &"{{table}}.{x}")

  for fq in fieldsQuery:
    let q = fq.query.toQs
    fieldsList.add(&"({q.query}) AS {fq.fieldAlias}")
    # add subquery params to query params
    if q.params.len != 0:
      self.params &= q.params
 
  self.fields &= self.extractFields(fieldsList)

  self.stmt.add(&"""SELECT {fieldsList.join(", ")}""")
  return self

proc select*(
  self: Sql,
  fields: openArray[string],
  #fieldsCase: openArray[tuple[caseCond: seq[tuple[cond: string, then: string]], fieldAlias: string]]): Sql =
  fieldsCase: openArray[tuple[caseCond: seq[tuple[cond: string, then: FieldItem]], fieldAlias: string]]): Sql =
  
  let fields = self.fields.map(proc (x: string): string = &"{{table}}.{x}")

  var fieldsList: seq[string]
  if fields.len > 0:
    fieldsList &= fields.map(proc (x: string): string =
      result = x
      if not x.toLower.contains(" as "):
        result = &"{{table}}.{x}")

  var caseStmt: seq[string]
  #var caseParams: seq[string]
  var caseParams: seq[FieldItem]
  for fc in fieldsCase:
    caseStmt = @[]
    caseParams = @[]
    for cc in fc.caseCond:
      if caseStmt.len == 0: caseStmt.add("CASE")
      caseStmt.add(&" WHEN {cc.cond} THEN ?")
      if cc.cond.toLower().strip == "else":
        caseStmt.add(&" ELSE ?")
      caseparams.add(cc.then)
    if caseStmt.len != 0:
      caseStmt.add(&" END AS {fc.fieldAlias}")

  if caseStmt.len != 0:
    fieldsList &= caseStmt

  self.fields &= self.extractFields(fieldsList)

  if caseParams.len != 0:
    self.params &= caseParams

  self.stmt.add(&"""SELECT {fieldsList.join(", ")}""")

  return self

proc fromTable*(
  self: Sql,
  table: string): Sql =
  
  self.fields = self.fields.map(proc (x: string): string = x.replace("{table}", table))
  self.stmt.add(&"""FROM {table}""")
  self.stmt[0] = self.stmt[0].replace("{table}", table)
  return self

proc fromSql*[T: string | Sql](
  self: Sql,
  query: T, params: varargs[string, `$`]): Sql =
  
  if T is string:
    self.stmt.add(&"FROM {cast[string](query)}")
  else:
    let q = cast[Sql](query).toQs
    self.stmt.add(&"FROM ({q.query})")
    if q.params.len != 0:
      self.params &= q.params

  if params.len != 0:
    self.params &= params

  return self

proc whereCond*[T: string | Sql](
  self: Sql,
  whereType: string,
  where: T,
  #params: varargs[string, `$`]): Sql =
  params: varargs[FieldItem]): Sql =
  
  if T is string:
    self.stmt.add(&"{whereType} {cast[string](where)}")
  else:
    let q = cast[Sql](where).toQs
    self.stmt.add(&"{whereType} ({q.query})")
    if q.params.len != 0:
      self.params &= q.params

  if params.len != 0:
    self.params &= params

  return self

proc where*[T: string | Sql](
  self: Sql,
  where: T,
  #params: varargs[string, `$`]): Sql =
  params: varargs[FieldItem]): Sql =
  
  return self.whereCond("WHERE", where, params)

proc whereExists*[T: string | Sql](
  self: Sql,
  where: T,
  #params: varargs[string, `$`]): Sql =
  params: varargs[FieldItem]): Sql =
  
  return self.whereCond("WHERE EXISTS", where, params)

proc andExists*[T: string | Sql](
  self: Sql,
  where: T,
  #params: varargs[string, `$`]): Sql =
  params: varargs[FieldItem]): Sql =
  
  return self.whereCond("AND EXISTS", where, params)

proc orExists*[T: string | Sql](
  self: Sql,
  where: T,
  #params: varargs[string, `$`]): Sql =
  params: varargs[FieldItem]): Sql =
  
  return self.whereCond("OR EXISTS", where, params)

proc andWhere*[T: string | Sql](
  self: Sql,
  where: T,
  #params: varargs[string, `$`]): Sql =
  params: varargs[FieldItem]): Sql =
  
  return self.whereCond("AND", where, params)

proc orWhere*[T: string | Sql](
  self: Sql,
  where: T,
  #params: varargs[string, `$`]): Sql =
  params: varargs[FieldItem]): Sql =
  
  return self.whereCond("OR", where, params)

proc likeCond*[T](
  self: Sql,
  cond: string,
  field: string,
  pattern: T): Sql =
  
  self.stmt.add(&"{cond} {field} LIKE {pattern}")
  return self

proc whereLike*[T](
  self: Sql,
  field: string,
  pattern: T): Sql =

  return self.likeCond("WHERE", field, pattern)

proc andLike*[T](
  self: Sql,
  field: string,
  pattern: T): Sql =
  
  return self.likeCond("AND", field, pattern)

proc orLike*[T](
  self: Sql,
  field: string,
  pattern: T): Sql =
  
  return self.likeCond("OR", field, pattern)

proc unionCond*(
  self: Sql,
  cond: string,
  unionWith: Sql): Sql =

  let q = unionwith.toQs
  self.stmt.add(&"UNION {cond} {q.query}")
  if q.params.len != 0:
    self.params &= q.params

  return self

proc union*(
  self: Sql,
  unionWith: Sql): Sql =

  return self.unionCond("", unionWith)

proc unionAll*(
  self: Sql,
  unionWith: Sql): Sql =

  return self.unionCond("All", unionWith)

proc whereInCond*[T](
  self: Sql,
  whereType: string,
  cond: string,
  field: string,
  params: T): Sql =
  
  if T isnot Sql:
    let inParams = cast[seq[FieldItem]](params)
    if inParams.len != 0:
      #let inStmtParams = inParams.map(proc (x: string): string = "?")
      var inStmtParams: seq[string] = @[]
      for i in 0..inParams.high:
        inStmtParams.add("?")
      self.stmt.add(&"""{whereType} {field} {cond} IN ({inStmtParams.join(", ")})""")
      self.params &= inParams
  else:
    let q = cast[Sql](params).toQs
    self.stmt.add(&"{whereType} {field} {cond} IN ({q.query})")
    if q.params.len != 0:
      self.params &= q.params

  return self

proc whereIn*[T](
  self: Sql,
  field: string,
  params: T): Sql =
  
  return self.whereInCond("WHERE", "", field, params)

proc andIn*[T](
  self: Sql,
  field: string,
  params: T): Sql =
  
  return self.whereInCond("AND", "", field, params)

proc orIn*[T](
  self: Sql,
  field: string,
  params: T): Sql =
  
  return self.whereInCond("OR", "", field, params)

proc andNotIn*[T](
  self: Sql,
  field: string,
  params: T): Sql =
  
  return self.whereInCond("AND", "NOT", field, params)

proc orNotIn*[T](
  self: Sql,
  field: string,
  params: T): Sql =
  
  return self.whereInCond("OR", "NOT", field, params)

proc betweenCond*(
  self: Sql,
  whereType: string,
  cond: string,
  field: string,
  #param: tuple[startVal: string, endVal: string]): Sql =
  param: tuple[startVal: FieldItem, endVal: FieldItem]): Sql =
  
  self.stmt.add(&"""{whereType} {field} {cond} BETWEEN {param.startVal} AND {param.endVal}""")
  return self

proc whereBetween*(
  self: Sql,
  field: string,
  #param: tuple[startVal: string, endVal: string]): Sql =
  param: tuple[startVal: FieldItem, endVal: FieldItem]): Sql =
  
  return self.betweenCond("WHERE", "", field, param)

proc andBetween*(
  self: Sql,
  field: string,
  #param: tuple[startVal: string, endVal: string]): Sql =
  param: tuple[startVal: FieldItem, endVal: FieldItem]): Sql =
  
  return self.betweenCond("AND", "", field, param)

proc orBetween*(
  self: Sql,
  field: string,
  #param: tuple[startVal: string, endVal: string]): Sql =
  param: tuple[startVal: FieldItem, endVal: FieldItem]): Sql =
  
  return self.betweenCond("OR", "", field, param)

proc andNotBetween*(
  self: Sql,
  field: string,
  #param: tuple[startVal: string, endVal: string]): Sql =
  param: tuple[startVal: FieldItem, endVal: FieldItem]): Sql =
  
  return self.betweenCond("AND", "NOT", field, param)

proc orNotBetween*(
  self: Sql,
  field: string,
  #param: tuple[startVal: string, endVal: string]): Sql =
  param: tuple[startVal: FieldItem, endVal: FieldItem]): Sql =
  
  return self.betweenCond("OR", "NOT", field, param)

proc limit*(
  self: Sql,
  limit: int64): Sql =

  self.stmt.add(&"LIMIT {limit}")
  return self

proc offset*(
  self: Sql,
  offset: int64): Sql =

  self.stmt.add(&"OFFSET {offset}")
  return self

proc groupBy*(
  self: Sql,
  fields: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""GROUP BY {fields.join(", ")}""")
  return self

proc orderByCond*(
  self: Sql,
  orderType: string,
  fields: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""ORDER BY {fields.join(", ")} {orderType}""")
  return self

proc descOrderBy*(
  self: Sql,
  fields: varargs[string, `$`]): Sql =
  
  return self.orderByCond("DESC", fields)

proc ascOrderBy*(
  self: Sql,
  fields: varargs[string, `$`]): Sql =
  
  return self.orderByCond("ASC", fields)

proc innerJoin*(
  self: Sql,
  table: string,
  joinOn: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""INNER JOIN {table} ON {joinOn.join(", ")}""")
  return self

proc leftJoin*(
  self: Sql,
  table: string,
  joinOn: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""LEFT JOIN {table} ON {joinOn.join(", ")}""")
  return self

proc rightJoin*(
  self: Sql,
  table: string,
  joinOn: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""RIGHT JOIN {table} ON {joinOn.join(", ")}""")
  return self

proc fullJoin*(
  self: Sql,
  table: string,
  joinOn: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""FULL OUTER JOIN {table} ON {joinOn.join(", ")}""")
  return self

proc having*(
  self: Sql,
  having: string,
  #params: varargs[string, `$`]): Sql =
  params: varargs[FieldItem]): Sql =
  
  self.stmt.add(&"""HAVING {having}""")
  if params.len != 0:
    self.params &= params

  return self

proc insert*(
  self: Sql,
  table: string,
  fields: varargs[string, `$`]): Sql =

  self.fields &= self.extractFields(fields)
  self.stmt.add(&"""INSERT INTO {table} ({fields.join(", ")})""")
  return self

proc values*(
  self: Sql,
  #values: varargs[seq[string]]): Sql =
  values: varargs[seq[FieldItem]]): Sql =

  if self.stmt[0].contains("INSERT"):
    var insertVal: seq[string] = @[]
    for v in values:
      #let val = v.map(proc (x: string): string = "?").join(", ")
      var val: seq[string] = @[]
      for fi in v:
        val.add("?")
      #insertVal.add(&"""({val})""")
      insertVal.add(&"""({val.join(", ")})""")
      self.params &= v
    self.stmt.add(&"""VALUES ({insertVal.join(" ,")})""")
  else:
    raise newException(ValueError, "multi values only for INSERT")

  return self

proc value*(
  self: Sql,
  #values: varargs[string, `$`]): Sql =
  values: varargs[FieldItem]): Sql =

  let stmt = self.stmt[0]
  if stmt.contains("INSERT") or stmt.contains("UPDATE"):
    if stmt.contains("INSERT"):
      #let insertVal = values.map(proc (x: string): string = "?").join(", ")
      var insertVal: seq[string] = @[]
      for fi in values:
        insertVal.add("?")
      #self.stmt.add(&"VALUES ({insertVal})")
      self.stmt.add(&"""VALUES ({insertVal.join(", ")})""")
    self.params &= values
  else:
    raise newException(ValueError, "values only for INSERT OR UPDATE.")
  return self

proc update*(
  self: Sql,
  table: string,
  fields: varargs[string, `$`]): Sql =

  self.fields &= self.extractFields(fields)
  let setFields = fields.map(proc (x: string): string = &"{x}=?").join(", ")
  self.stmt.add(&"""UPDATE {table} SET {setFields}""")
  return self

proc delete*(
  self: Sql,
  table: string): Sql =

  self.stmt.add(&"""DELETE FROM {table}""")
  return self

proc toDbType*(
  field: string,
  value: string): JsonNode =
  
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

proc toDbType*(
  field: string,
  nodeKind: JsonNodeKind,
  value: string): JsonNode =

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

proc toWhereQuery*(
  j: JsonNode,
  tablePrefix: string = "",
  #op: string = "AND"): tuple[where: string, params: seq[string]] =
  op: string = "AND"): tuple[where: string, params: seq[FieldItem]] =

  var where: seq[string] = @[]
  #var whereParams: seq[string] = @[]
  var whereParams: seq[FieldItem] = @[]
  for k, v in j:
    if v.kind == JNull: continue
    where.add(if tablePrefix == "": &"{k}=?" else: &"{tablePrefix}.{k}=?")
    #whereParams.add(if v.kind == JString: v.getStr else: $v)
    whereParams.add(if v.kind == JString: (v.getStr, v.kind) else: ($v, v.kind))

  return (where.join(&" {op} "), whereParams)

proc toWhereQuery*[T](
  obj: T,
  tablePrefix: string = "",
  #op: string = "AND"): tuple[where: string, params: seq[string]] =
  op: string = "AND"): tuple[where: string, params: seq[FieldItem]] =

  return (%obj).toWhereQuery(tablePrefix, op)
