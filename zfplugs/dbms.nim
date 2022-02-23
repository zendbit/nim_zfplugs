##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

import zfdbms/dbms
export dbms

when WITH_MYSQL or WITH_PGSQL or WITH_SQLITE:
  import
    strformat,
    times,
    macros,
    tables,
    typetraits,
    strutils,
    sequtils,
    json,
    options,
    re

  export
    options,
    strutils,
    sequtils,
    json,
    strformat

  import
    stdext/[
      xjson,
      xstrutils,
      xsystem],
    settings,
    zfdbms/[
      dbs,
      dbssql]

  export
    dbs,
    dbssql,
    xjson,
    xstrutils,
    xsystem

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
          result = DBMS[T](
            connId: connId,
            dbmsStack: initTable[DbmsStmtType, DbmsFieldType]())
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

