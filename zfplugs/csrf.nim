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
import std/sha1
import std/random
import macros

import zfcore/httpcontext
import zfplugs/session

proc generateCsrf*(ctx: HttpContext): string {.gcsafe.} =
  ##
  ##  generate csrf:
  ##
  ##  generate csrf token string, return unique csrf token
  ##
  let tokenMD5 = ($(($rand(999999999)).secureHash)).getMD5
  let token = tokenMD5.subStr(tokenMD5.len - 6)
  var csrfData = %*{"token": token}
  ctx.addCookieSession("csrf", csrfData)
  result = token

proc isCsrfValid*(ctx: HttpContext, token: string): bool {.gcsafe.} =
  ##
  ##  check if csrf valid:
  ##
  ##  check given csrf token is valid or not, if valid return true.
  ##

  let csrfData = ctx.getCookieSession("csrf")
  result = not csrfData.isNil and not csrfData{"token"}.isNil and csrfData{"token"}.getStr == token

proc destroyCsrf*(ctx: HttpContext) {.gcsafe.} =
  ##
  ##  delete the csrf token:
  ##
  ##  delete given csrf token.
  ##
  ctx.deleteCookieSession("csrf")

##
##  macro for csrf
##
##  auto pass ctx to function
##
macro generateCsrf*(): string =
  ##
  ##  macro for generate csrf
  ##  auto call ctx
  ##
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("generateCsrf")
    )
  )

macro isCsrfValid*(token: string): bool =
  ##
  ##  macro check if csrf token valid
  ##
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("isCsrfValid")
    ),
    token
  )

macro destroyCsrf*() =
  ##
  ##  macro check if csrf token valid
  ##
  nnkCall.newTree(
    nnkDotExpr.newTree(
      newIdentNode("ctx"),
      newIdentNode("destroyCsrf")
    )
  )
