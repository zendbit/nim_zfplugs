##
##  helper for layout system using mustache
##  https://github.com/soasme/nim-mustache
##
import mustache
export mustache
import os, strformat, strutils

type
  ZView* = ref object of RootObj
    html: string
    c*: Context

proc newViewFromFile*(name: string, searchDirs: seq[string] = @["./"]): ZView =
  ##
  ## Create new layout, pass layout file path to load
  ##
  let templateDir = getAppDir().joinPath("template")
  let searchTemplates = searchDirs & @[templateDir];
  let l = ZView()
  var layoutPath = templateDir.joinPath(name.replace("::", $DirSep))
  if not layoutPath.fileExists:
    if templateDir.dirExists:
      layoutPath = templateDir.joinPath(name)

  if layoutPath.fileExists:
    let readHtml = open(layoutPath, fmRead)
    l.html = readHtml.readAll
    readHtml.close
  else:
    l.html = &"Layout file not found {layoutPath}"
  l.c = newContext(searchDirs = searchTemplates)
  result = l

proc newView*(tpl: string, searchDirs: seq[string] = @["./"]): ZView =
  ##
  ## Create new layout, pass layout template string
  ##
  let templateDir = getAppDir().joinPath("template")
  let searchTemplates = searchDirs & @[templateDir];
  result = ZView(html: tpl, c: newContext(searchDirs = searchTemplates))

proc clear*(view: ZView) =
  ##
  ##  Clear layout context parameter
  ##
  view.c = newContext()

proc render*(view: ZView): string =
  ##
  ##  Render content with given context
  ##
  ##  let l = newLayout(
  ##    """Hello {{name}}
  ##    You have just won {{value}} dollars!
  ##    {{#in_ca}}
  ##    Well, {{taxed_value}} dollars, after taxes.
  ##    {{/in_ca}}"""
  ##  )
  ##
  ##  l.c["name"] = "Chris"
  ##  l.c["value"] = 10000
  ##  l.c["taxed_value"] = 10000 - (10000 * 0.4)
  ##  l.c["in_ca"] = true
  ##
  ##  echo l.render
  ##
  result = render(view.html, view.c)
