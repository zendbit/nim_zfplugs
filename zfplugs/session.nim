#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

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
  #
  # check if session exists with given session token
  #
  return sessionDir.joinPath(sessionToken).existsFile

proc writeSession(
  sessionToken: string,
  data: JsonNode): bool {.discardable gcsafe.} =
  #
  # write session data with given session token and data
  #
  let f = sessionDir.joinPath(sessionToken).open(fmWrite)
  f.write(xorEncodeDecode($data, sessionToken))
  f.close
  result = sessionToken.isSessionExists

proc createSessionToken(): string {.gcsafe.} =
  #
  # the session token will be used for accessing the session
  #
  let token = $secureHash(now().utc().format("YYYY-MM-dd HH:mm:ss:fffffffff"))
  token.writeSession(%*{})
  return token

proc createSessionFromToken(token: string): bool {.gcsafe.} =
  #
  # this will generate session with given token and sessionAge in seconds.
  # will check if session conflict with other session or not
  # the session token will be used for accessing the session
  #
  if not token.isSessionExists:
    token.writeSession(%*{})

proc readSession(sessionToken: string): JsonNode {.gcsafe.} =
  #
  # read session data with given token
  #
  if sessionToken.isSessionExists:
    let f = sessionDir.joinPath(sessionToken).open
    result = f.readAll().xorEncodeDecode(sessionToken).parseJson
    f.close

proc getSession*(
  ctx: HttpContext,
  key: string): JsonNode {.gcsafe.} =
  #
  # get session value with given session token and key
  #
  let sessionData = ctx.getCookie().getOrDefault(sessid).readSession()
  if not sessionData.isNil:
    result = sessionData{key}

proc addSession*(
  ctx: HttpContext,
  key: string, value: JsonNode) {.gcsafe.} =
  #
  # add session
  #
  let cookie = ctx.getCookie
  var token = cookie.getOrDefault(sessid)
  if token != "":
    token = cookie[sessid]

  else:
    token = createSessionToken()
    ctx.setCookie({sessid: token}.newStringTable)

  let sessionData = token.readSession
  if not sessionData.isNil:
    sessionData[key] = value
    token.writeSession(sessionData)

proc deleteSession*(
  ctx: HttpContext,
  key: string) {.gcsafe.} =
  #
  # delete session value with given session token and key
  #
  let sessionData = ctx.getCookie().getOrDefault(sessid).readSession
  if not sessionData.isNil:
    if sessionData.hasKey(key):
      sessionData.delete(key)

proc destroySession*(ctx: HttpContext) {.gcsafe.} =
  #
  # destroy session data
  #
  var cookie = ctx.getCookie
  let token = cookie.getOrDefault(sessid)
  if token != "":
    sessionDir.joinPath(token).removeFile
    cookie.del(sessid)
    ctx.setCookie(cookie)

macro addSession*(
  key: string,
  value: JsonNode) =
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("addSession")
    ),
    key,
    value
  )

macro getSession*(key: string): untyped =
  return nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("getSession")
    ),
    key
  )

macro deleteSession*(key: string): untyped =
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("deleteSession")
    ),
    key
  )

macro destroySession*(): untyped =
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("destroySession")
    )
  )

