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

let jsonSettingsFile = joinPath(getAppDir(), "settings.json")
var jsonSettings* {.threadvar.}: JsonNode
if existsFile(jsonSettingsFile):
  try:
    echo "settings.json found."
    jsonSettings = parseFile(jsonSettingsFile)

  except Exception as ex:
    echo ex.msg

else:
  echo "settings.json not found!!."
  echo "load default zfcore settings."
  jsonSettings = %zfcoreInstance.settings
