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
import zfplugs/session

proc genCsrf*(): string {.gcsafe.} =
  ##
  ##  generate csrf:
  ##
  ##  generate csrf token string, return unique csrf token
  ##
  result = newSession(%*{}, 3600)

proc isCsrfValid*(token: string): bool {.gcsafe.} =
  ##
  ##  check if csrf valid:
  ##
  ##  check given csrf token is valid or not, if valid return true.
  ##
  result = token.isSessionExists

proc destroyCsrf*(token: string) {.gcsafe.} =
  ##
  ##  delete the csrf token:
  ##
  ##  delete given csrf token.
  ##
  token.destroySession
