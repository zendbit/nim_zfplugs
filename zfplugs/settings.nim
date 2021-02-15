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

proc jsonSettings*(): JsonNode {.gcsafe.} =
  result = zfJsonSettings()
  if result.len == 0:
    result = %settings
