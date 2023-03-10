##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

import os, json
import zfcore/server

export json

var settings {.threadvar.}: Settings
settings.deepCopy(zfcoreInstance.settings)

proc appSettings*(): Settings {.gcsafe.} =
  result = settings

proc jsonSettings*(): JsonNode {.gcsafe.} =
  ##
  ##  json settings:
  ##
  ##  get value of zfcore settings stored in settings.json.
  ##
  result = zfJsonSettings()
  if result.len == 0:
    result = %settings
