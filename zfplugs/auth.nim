##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit
##

import base64
import strutils
import httpcore
import json

from zfcore/server import getValues
import session

proc validateBasicAuth*(
  httpHeaders: HttpHeaders,
  username: string,
  password: string): bool {.gcsafe.} =
  ##
  ##  validate basic auth
  ##
  ##  will check the username and password with httpHeaders,
  ##  if username and password in headers valid with given username and password return true.
  ##
  let auth = httpHeaders.getValues("Authorization").split(" ")
  if auth.len() == 2 and auth[0].toLower.strip == "basic":
    let userPass = auth[1].decode().split(":")
    if userPass.len() == 2:
      result = userPass[0] == username and userPass[1] == password

proc validateBearerAuth*(
  httpHeaders: HttpHeaders,
  token: string): bool {.gcsafe.} =
  ##
  ##  validate bearer
  ##
  ##  make sure token same with bearer Auth from header
  ##

  let auth = httpHeaders.getValues("Authorization").split(" ")
  if auth.len() == 2 and auth[0].toLower.strip == "bearer":
    result = auth[1].strip == token.strip

proc validateMagicStr*(
  httpHeaders: HttpHeaders): bool {.gcsafe.} =
  ##
  ##  validate Magic-String headers
  ##
  ##  this coop with bearer token make it pair and secure
  ##  magic str will destroy after calling validateMagicStr
  ##

  let mStr = httpHeaders.getValues("Magic-String").strip
  result = mStr.isSessionExists
  mStr.destroySession

proc generateMagicStr*(): string =
  ##
  ##  generate magic str
  ##
  ##  this is only valid fo 120 seconds/2 minute
  ##
  result = newSession(%*{}, 120)

