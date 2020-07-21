#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import db_mysql, strformat, json

import dbs, settings

type
  MySql* = ref object
    connId: string
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
proc newMySql*(connId: string): MySql =
  let jsonSettings = jsonSettings()
  if not jsonSettings.isNil:
    let db = jsonSettings{"database"}
    if not db.isNil:
      let dbConf = db{connId}
      if not dbConf.isNil:
        result = MySql(connId: connId)
        let c = newDbs(
          dbConf{"database"}.getStr(),
          dbConf{"username"}.getStr(),
          dbConf{"password"}.getStr(),
          dbConf{"host"}.getStr(),
          dbConf{"port"}.getInt()).tryMySqlConn()

        if c.success:
          result.conn = c.conn
        else:
          echo c.msg

      else:
        echo &"database {connId} not found!!."

    else:
      echo "database section not found!!."

# test ping the server
proc ping*(conn: DbConn): bool =
  try:
    conn.exec(sql "SELECT 1")
    result = true
  except:
    discard

# get connId
proc connId*(self: MySql): string =
  if not self.isNil:
    result = self.connId

# get dbconn
proc conn*(self: MySql): DbConn =
  if not self.isNil:
    result = self.conn

export
  db_mysql
