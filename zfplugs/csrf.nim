##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

# csrf generator and manager
import json
import std/md5

import zfplugs/session

type
  CSRF* = ref object of RootObj
    token*: string
    uuid*: string
    expired*: int64

proc newCSRF*(expired: int64 = 3600): CSRF {.gcsafe.} =
  ##
  ##  generate csrf:
  ##
  ##  generate csrf token string, return unique csrf token
  ##
  let token = expired.createSessionToken
  let tokenMD5 = token.getMD5
  let csrf = CSRF(
    token: token,
    uuid: tokenMD5.substr(tokenMD5.len - 6),
    expired: expired
  )

  if token.writeSession(%csrf):
    result = csrf

proc isCsrfValid*(token: string, uuid: string): bool {.gcsafe.} =
  ##
  ##  check if csrf valid:
  ##
  ##  check given csrf token is valid or not, if valid return true.
  ##
  if token.isSessionExists:
    let data = token.readSession
    let id = data{"uuid"}
    result = (not id.isNil) and (id.getStr == uuid)

proc destroyCsrf*(token: string) {.gcsafe.} =
  ##
  ##  delete the csrf token:
  ##
  ##  delete given csrf token.
  ##
  token.destroySession
