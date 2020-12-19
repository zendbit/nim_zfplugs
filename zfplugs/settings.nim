#[
  zfcore web framework for nim language
  This framework if free to use and to modify
  License: BSD
  Author: Amru Rosyada
  Email: amru.rosyada@gmail.com
  Git: https://github.com/zendbit
]#

import os, json
import zfcore

# thead safe
var settings {.threadvar.}: Settings
settings.deepCopy(zfcoreInstance.settings)

proc jsonSettings*(): JsonNode {.gcsafe.} =
  let jsonSettingsFile = joinPath(getAppDir(), "settings.json")
  if existsFile(jsonSettingsFile):
    try:
      result = parseFile(jsonSettingsFile)

    except Exception as ex:
      echo ex.msg

  else:
    result = %settings
