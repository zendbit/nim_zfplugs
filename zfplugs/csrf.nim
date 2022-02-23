##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

# csrf generator and manager
import
  dbs,
  db_sqlite,
  times,
  std/sha1,
  os,
  strutils,
  asyncdispatch

import zfcore/server
from stdext/encryptx import xorEncodeDecode

var csrfDir {.threadvar.}: string
csrfDir = zfcoreInstance.settings.tmpDir.joinPath("csrf")
if not csrfDir.existsDir:
  csrfDir.createDir

if csrfDir.existsDir:
  zfcoreInstance.settings.addTmpCleanupDir("csrf")

proc genCsrf*(): string {.gcsafe.} =
  ##
  ##  generate csrf:
  ##
  ##  generate csrf token string, return unique csrf token
  ##
  let tokenSeed = now().utc.format("yyyy-MM-dd HH:mm:ss:fffffffff".initTimeFormat)
  let token = $secureHash(tokenSeed)
  let f = csrfDir.joinPath(token).open(fmWrite)
  f.write("")
  f.close
  result = token

proc isCsrfValid*(token: string): bool {.gcsafe.} =
  ##
  ##  check if csrf valid:
  ##
  ##  check given csrf token is valid or not, if valid return true.
  ##
  result = csrfDir.joinPath(token).existsFile

proc delCsrf*(token: string) {.gcsafe.} =
  ##
  ##  delete the csrf token:
  ##
  ##  delete given csrf token.
  ##
  csrfDir.joinPath(token).removeFile
