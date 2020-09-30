import db_common, sequtils, strformat, strutils

type
  Sql* = ref object
    stmt: seq[string]
    params: seq[string]

proc toQ*(self: Sql): tuple[query: SqlQuery, params: seq[string]] =
  
  return (sql self.stmt.join(" "), self.params)

proc toQs*(self: Sql): tuple[query: string, params: seq[string]] =
  
  return (self.stmt.join(" "), self.params)

proc select*(
  self: Sql,
  fields: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""SELECT {fields.join(", ")}""")
  return self

proc select*(
  self: Sql,
  fields: openArray[string],
  fieldsQuery: openArray[tuple[query: Sql, fieldAlias: string]]): Sql =
  
  var fieldsList: seq[string]
  if fields.len > 0:
    fieldsList &= fields

  for fq in fieldsQuery:
    let q = fq.query.toQs
    fieldsList.add(&"({q.query}) AS {fq.fieldAlias}")
    # add subquery params to query params
    if q.params.len != 0:
      self.params &= q.params

  self.stmt.add(&"""SELECT {fieldsList.join(", ")}""")
  return self

proc select*(
  self: Sql,
  fields: openArray[string],
  fieldsCase: openArray[tuple[caseCond: seq[tuple[cond: string, then: string]], fieldAlias: string]]): Sql =
  
  var fieldsList: seq[string]
  if fields.len > 0:
    fieldsList &= fields

  var caseStmt: seq[string]
  var caseParams: seq[string]
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

  if caseParams.len != 0:
    self.params &= caseParams

  self.stmt.add(&"""SELECT {fieldsList.join(", ")}""")

  return self

proc fromTable*(
  self: Sql,
  tables: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""FROM {tables.join(", ")}""")
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

proc whereCond[T: string | Sql](
  self: Sql,
  whereType: string,
  where: T, params: varargs[string, `$`]): Sql =
  
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
  params: varargs[string, `$`]): Sql =
  
  return self.whereCond("WHERE", where, params)

proc whereExists*[T: string | Sql](
  self: Sql,
  where: T,
  params: varargs[string, `$`]): Sql =
  
  return self.whereCond("WHERE EXISTS", where, params)

proc andExists*[T: string | Sql](
  self: Sql,
  where: T,
  params: varargs[string, `$`]): Sql =
  
  return self.whereCond("AND EXISTS", where, params)

proc orExists*[T: string | Sql](
  self: Sql,
  where: T,
  params: varargs[string, `$`]): Sql =
  
  return self.whereCond("OR EXISTS", where, params)

proc andWhere*[T: string | Sql](
  self: Sql,
  where: T,
  params: varargs[string, `$`]): Sql =
  
  return self.whereCond("AND", where, params)

proc orWhere*[T: string | Sql](
  self: Sql,
  where: T,
  params: varargs[string, `$`]): Sql =
  
  return self.whereCond("OR", where, params)

proc likeCond[T](
  self: Sql,
  cond: string,
  field: string,
  pattern: T): Sql =
  
  self.stmt.add(&"{cond} {field} LIKE '{pattern}'")
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

proc unionCond(
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

proc whereInCond[T](
  self: Sql,
  whereType: string,
  cond: string,
  field: string,
  params: T): Sql =
  
  if T isnot Sql:
    let inParams = cast[seq[string]](params)
    if inParams.len != 0:
      let inStmtParams = inParams.map(proc (x: string): string = "?")
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

proc betweenCond(
  self: Sql,
  whereType: string,
  cond: string,
  field: string,
  param: tuple[startVal: string, endVal: string]): Sql =
  
  self.stmt.add(&"""{whereType} {field} {cond} BETWEEN {param.startVal} AND {param.endVal}""")
  return self

proc whereBetween*(
  self: Sql,
  field: string,
  param: tuple[startVal: string, endVal: string]): Sql =
  
  return self.betweenCond("WHERE", "", field, param)

proc andBetween*(
  self: Sql,
  field: string,
  param: tuple[startVal: string, endVal: string]): Sql =
  
  return self.betweenCond("AND", "", field, param)

proc orBetween*(
  self: Sql,
  field: string,
  param: tuple[startVal: string, endVal: string]): Sql =
  
  return self.betweenCond("OR", "", field, param)

proc andNotBetween*(
  self: Sql,
  field: string,
  param: tuple[startVal: string, endVal: string]): Sql =
  
  return self.betweenCond("AND", "NOT", field, param)

proc orNotBetween*(
  self: Sql,
  field: string,
  param: tuple[startVal: string, endVal: string]): Sql =
  
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

proc orderByCond(
  self: Sql,
  orderType: string, fields: varargs[string, `$`]): Sql =
  
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
  params: varargs[string, `$`]): Sql =
  
  self.stmt.add(&"""HAVING {having}""")
  if params.len != 0:
    self.params &= params

  return self
