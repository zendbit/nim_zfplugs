##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

import std.sha1, macros
import zfcore, stdext/[encrypt_ext]

const sessid = "_zfsid"

var sessionDir {.threadvar.}: string
sessionDir = zfcoreInstance.settings.tmpDir.joinPath("session")
if not sessionDir.existsDir:
  sessionDir.createDir

if sessionDir.existsDir:
  # keep the session for month
  zfcoreInstance.settings.addTmpCleanupDir("session", 2592000)

proc isSessionExists(sessionToken: string): bool {.gcsafe.} =
  ##
  ##  check if session already exists:
  ##
  ##  this will check the session token.
  ##
  result = sessionDir.joinPath(sessionToken).existsFile

proc writeSession(
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

proc createSessionToken(): string {.gcsafe.} =
  ##
  ##  create sossion token:
  ##
  ##  create string token session.
  ##
  let token = $secureHash(now().utc().format("YYYY-MM-dd HH:mm:ss:fffffffff"))
  token.writeSession(%*{})
  result = token

proc createSessionFromToken(token: string): bool {.gcsafe.} =
  ##
  ##  create session from token:
  ##
  ##  this will generate session with given token and sessionAge in seconds.
  ##  will check if session conflict with other session or not
  ##  the session token will be used for accessing the session
  ##
  if not token.isSessionExists:
    token.writeSession(%*{})

proc readSession(sessionToken: string): JsonNode {.gcsafe.} =
  ##
  ##  read session:
  ##
  ##  read session data with given token.
  ##
  if sessionToken.isSessionExists:
    let f = sessionDir.joinPath(sessionToken).open
    result = f.readAll().xorEncodeDecode(sessionToken).parseJson
    f.close

proc getSession*(
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

proc addSession*(
  ctx: HttpContext,
  key: string, value: JsonNode) {.gcsafe.} =
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
    ctx.setCookie({sessid: token}.newStringTable)

  if sessionData.isNil:
    discard token.createSessionFromToken()
  
  sessionData = token.readSession
  if not sessionData.isNil:
    sessionData[key] = value
    token.writeSession(sessionData)

proc deleteSession*(
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

proc destroySession*(ctx: HttpContext) {.gcsafe.} =
  ##
  ##  destory session:
  ##
  ##  will destroy all session key and data from zfcore HttpContext.
  ##
  var cookie = ctx.getCookie
  let token = cookie.getOrDefault(sessid)
  if token != "":
    sessionDir.joinPath(token).removeFile
    cookie.del(sessid)
    ctx.setCookie(cookie)

macro addSession*(
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
      newIdentNode("addSession")
    ),
    key,
    value
  )

macro getSession*(key: string): untyped =
  ##
  ##  get session macro:
  ##
  ##  will return session value from zfcore HttpContext.
  ##
  return nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("getSession")
    ),
    key
  )

macro deleteSession*(key: string): untyped =
  ##
  ##  delete session macro:
  ##
  ##  will delete session value with given key from zfcore HttpContext.
  ##
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("deleteSession")
    ),
    key
  )

macro destroySession*(): untyped =
  ##
  ##  destroy session macro:
  ##
  ##  will destroy all session key and value from zfcore HttpContext.
  ##
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("destroySession")
    )
  )

