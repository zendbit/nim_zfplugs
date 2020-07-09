#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import std/sha1
import zfcore, stdext.encrypt_ext

let sessionDir = zfcoreInstance.settings.tmpDir.joinPath("session")
if not sessionDir.existsDir:
  sessionDir.createDir

if sessionDir.existsDir:
  zfcoreInstance.settings.addTmpCleanupDir("session")

proc isSessionExists*(sessionToken: string): bool =
  #
  # check if session exists with given session token
  #
  return sessionDir.joinPath(sessionToken).existsFile

proc writeSession(sessionToken: string, data: JsonNode): bool {.discardable.} =
  #
  # write session data with given session token and data
  #
  let f = sessionDir.joinPath(sessionToken).open(fmWrite)
  f.write(xorEncodeDecode($data, sessionToken))
  f.close
  result = sessionToken.isSessionExists

proc createSessionToken*(sessionAge: int64 = 3600): string =
  #
  # this will generate unique token for the session with given sessionAge in seconds.
  # default sessionAge is 3600 seconds or 1 hour
  # the session token will be used for accessing the session
  #
  let token = $secureHash(now().utc().format("YYYY-MM-dd HH:mm:ss:fffffffff"))
  token.writeSession(%*{"maxAge": sessionAge})
  return token

proc createSessionFromToken*(token: string, sessionAge: int64 = 3600): bool =
  #
  # this will generate session with given token and sessionAge in seconds.
  # will check if session conflict with other session or not
  # default sessionAge is 3600 seconds or 1 hour
  # the session token will be used for accessing the session
  #
  if not token.isSessionExists:
    token.writeSession(%*{"maxAge": sessionAge})

proc readSession*(sessionToken: string): JsonNode =
  #
  # read session data with given token
  #
  if sessionToken.isSessionExists:
    let f = sessionDir.joinPath(sessionToken).open
    result = f.readAll().xorEncodeDecode(sessionToken).parseJson
    f.close

proc getSession*(sessionToken: string, key: string): JsonNode =
  #
  # get session value with given session token and key
  #
  let sessionData = sessionToken.readSession()
  if not sessionData.isNil:
    result = sessionData{key}

proc addSession*(sessionToken: string, key: string, value: JsonNode): bool {.discardable.} =
  #
  # add session
  #
  let sessionData = sessionToken.readSession
  if not sessionData.isNil:
    sessionData[key] = value
    sessionToken.writeSession(sessionData)
    result = not sessionToken.getSession(key).isNil

proc deleteSession*(sessionToken: string, key: string): bool {.discardable.} =
  #
  # delete session value with given session token and key
  #
  let sessionData = sessionToken.readSession
  if not sessionData.isNil:
    if sessionData.hasKey(key):
      sessionData.delete(key)
      result = sessionToken.getSession(key).isNil

proc destroySession*(sessionToken: string): bool {.discardable.} =
  #
  # destroy session data
  #
  sessionDir.joinPath(sessionToken).removeFile
  return sessionToken.isSessionExists

