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

proc basicAuthHeader(httpHeaders: HttpHeaders): tuple[username: string, password: string] =
  let auth = httpHeaders.getValues("Authorization").strip().split(" ")
  if auth.len() == 2 and auth[0].toLower.strip == "basic":
    let userPass = auth[1].decode().split(":")
    if userPass.len() == 2:
      result = (userPass[0], userPass[1])


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
  let authHeader = basicAuthHeader(httpHeaders)
  result = authHeader.username == username and authHeader.password == password

proc bearerAuthHeader*(httpHeaders: HttpHeaders): string =
  let auth = httpHeaders.getValues("Authorization").strip().split(" ")
  if auth.len() == 2 and auth[0].toLower.strip == "bearer":
    result = auth[1].strip

proc validateBearerAuth*(
  httpHeaders: HttpHeaders,
  token: string): bool {.gcsafe.} =
  ##
  ##  validate bearer
  ##
  ##  make sure token same with bearer Auth from header
  ##

  let authHeader = bearerAuthHeader(httpHeaders)
  result = authHeader == token.strip


