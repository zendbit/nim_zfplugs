##
##  helper for layout system using mustache
##  https://github.com/soasme/nim-mustache
##
import mustache
export mustache
import os, strformat, strutils

type
  Layout* = ref object
    html: string
    c*: Context

proc newLayoutFromFile*(name: string): Layout =
  ##
  ## Create new layout, pass layout file path to load
  ##
  let templateDir = getAppDir().joinPath("template")
  let l = Layout()
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
  l.c = newContext()
  result = l

proc newLayout*(tpl: string): Layout =
  ##
  ## Create new layout, pass layout template string
  ##
  result = Layout(html: tpl, c: newContext())

proc clear*(layout: Layout) =
  ##
  ##  Clear layout context parameter
  ##
  layout.c = newContext()

proc render*(layout: Layout): string =
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
  result = render(layout.html, layout.c)
