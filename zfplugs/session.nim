##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

import
  std/sha1,
  macros,
  std/random,
  strutils,
  parseutils

import
  zfcore/server,
  stdext/xencrypt

const sessid = "_zfsid"

var sessionDir {.threadvar.}: string
sessionDir = zfcoreInstance.settings.tmpDir.joinPath("session")
if not sessionDir.existsDir:
  sessionDir.createDir

if sessionDir.existsDir:
  ##
  ##  register session checking to tasksPool
  ##
  zfcoreInstance.tasksPool["session"] = TasksPoolAction(
    action: proc (param: JsonNode) =
      for file in param.getStr.joinPath("*").walkFiles:
        let fileInfo = file.extractFilename.split("-")
        let fileInfoLen = fileInfo.len
        ##
        ##  session format is
        ##  token_expirationtime
        ##  need split with "-" to get expirationtime
        ##
        var expiredTime: int64
        if fileInfoLen >= 2 and
          fileInfo[fileInfoLen - 1].parseBiggestInt(expiredTime) != 0 and
          now().utc.toTime.toUnix < expiredTime:
          continue

        file.removeFile
    ,
    param: %sessionDir
  )

proc generateExpired(expired: int64): int64 =
  result = now().utc.toTime.toUnix + expired

proc isSessionExists*(sessionToken: string): bool {.gcsafe.} =
  ##
  ##  check if session already exists:
  ##
  ##  this will check the session token.
  ##
  result = sessionDir.joinPath(sessionToken).existsFile

proc writeSession*(
  sessionToken: string,
  data: JsonNode): bool {.discardable gcsafe.} =
  ##
  ##  write session data:
  ##
  ##  the session data information in json format and will encrypted for security reason.
  ##
  let f = sessionDir.joinPath(sessionToken).open(fmWrite)
  f.write(xorEncodeDecode($data, sessionToken))
  f.close
  result = sessionToken.isSessionExists

proc createSessionToken*(expired: int64 = 2592000): string {.gcsafe.} =
  ##
  ##  create sossion token:
  ##
  ##  create string token session.
  ##
  let token = $secureHash(now().utc().format("YYYY-MM-dd HH:mm:ss:fffffffff") & $rand(1000000000)) & "-" & $expired.generateExpired()
  token.writeSession(%*{})
  result = token

proc createSessionFromToken*(token: string): bool {.gcsafe.} =
  ##
  ##  create session from token:
  ##
  ##  this will generate session with given token and sessionAge in seconds.
  ##  will check if session conflict with other session or not
  ##  the session token will be used for accessing the session
  ##
  if not token.isSessionExists:
    token.writeSession(%*{})

proc newSession*(data: JsonNode, expired: int64 = 2592000): string {.gcsafe.} =
  ##
  ##  create new session on server will return token access
  ##
  ##  token access is needed for retrieve, read, write and store the data
  ##  need to remember the token if using server side session
  ##  for cookie session token will manage by browser token will save on the cookie session
  ##
  let token = createSessionToken(expired)
  if token.isSessionExists:
    token.writeSession(data)

  result = token

proc readSession*(sessionToken: string): JsonNode {.gcsafe.} =
  ##
  ##  read session:
  ##
  ##  read session data with given token.
  ##
  if sessionToken.isSessionExists:
    let f = sessionDir.joinPath(sessionToken).open
    result = f.readAll().xorEncodeDecode(sessionToken).parseJson
    f.close

proc destroySession*(sessionToken: string) {.gcsafe.} =
  ##
  ##  read session:
  ##
  ##  read session data with given token.
  ##
  if sessionToken.isSessionExists:
    sessionDir.joinPath(sessionToken).removeFile

proc getCookieSession*(
  ctx: HttpContext,
  key: string): JsonNode {.gcsafe.} =
  ##
  ##  get session:
  ##
  ##  get session value with given key from zfcore HttpContext.
  ##
  let sessionData = ctx.getCookie().getOrDefault(sessid).readSession()
  if not sessionData.isNil:
    result = sessionData{key}
  if result.isNil:
    result = %*{}

proc addCookieSession*(
  ctx: HttpContext,
  key: string, value: JsonNode,
  domain: string = "",
  path: string = "/",
  expires: string = "",
  secure: bool = false) {.gcsafe.} =
  ##
  ##  add session:
  ##
  ##  add session data to zfcore HttpContext. If key exists will overwrite existing data.
  ##
  var sessionData: JsonNode
  let cookie = ctx.getCookie
  var token = cookie.getOrDefault(sessid)
  if token != "":
    token = cookie[sessid]
    sessionData = token.readSession

  else:
    token = createSessionToken()
    ctx.setCookie({sessid: token}.newStringTable, domain, path, expires, secure)

  if sessionData.isNil:
    discard token.createSessionFromToken()
  
  sessionData = token.readSession
  if not sessionData.isNil:
    sessionData[key] = value
    token.writeSession(sessionData)

proc deleteCookieSession*(
  ctx: HttpContext,
  key: string) {.gcsafe.} =
  ##
  ##  delete session:
  ##
  ##  delete session data with given key from zfcore HttpContext.
  ##
  let sessionData = ctx.getCookie().getOrDefault(sessid).readSession
  if not sessionData.isNil:
    if sessionData.hasKey(key):
      sessionData.delete(key)

proc destroyCookieSession*(ctx: HttpContext) {.gcsafe.} =
  ##
  ##  destory session:
  ##
  ##  will destroy all session key and data from zfcore HttpContext.
  ##
  var cookie = ctx.getCookie
  let token = cookie.getOrDefault(sessid)
  if token != "":
    token.destroySession
    cookie.del(sessid)
    ctx.setCookie(cookie)

macro addCookieSession*(
  key: string,
  value: JsonNode) =
  ##
  ##  add session macro:
  ##
  ##  create and add session data to zfcore HttpContext.
  ##
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("addCookieSession")
    ),
    key,
    value
  )

macro getCookieSession*(key: string): untyped =
  ##
  ##  get session macro:
  ##
  ##  will return session value from zfcore HttpContext.
  ##
  return nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("getCookieSession")
    ),
    key
  )

macro deleteCookieSession*(key: string): untyped =
  ##
  ##  delete session macro:
  ##
  ##  will delete session value with given key from zfcore HttpContext.
  ##
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("deleteCookieSession")
    ),
    key
  )

macro destroyCookieSession*(): untyped =
  ##
  ##  destroy session macro:
  ##
  ##  will destroy all session key and value from zfcore HttpContext.
  ##
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("destroyCookieSession")
    )
  )

