##
##  zfcore web framework for nim language
##  This framework if free to use and to modify
##  License: BSD
##  Author: Amru Rosyada
##  Email: amru.rosyada@gmail.com
##  Git: https://github.com/zendbit/nim.zfplugs
##

import
  uri3,
  json,
  math,
  strutils,
  strformat

proc genPaging*(
  data: JsonNode,
  url: Uri3,
  perPage: int64,
  numData: int64): JsonNode {.gcsafe.} =
  ##
  ##  generate paging:
  ##
  ##  simply will calculate given parameter to return paging url and number of page.
  ##  the return win JsonNode format
  ##  {
  ##    "pageData": [the data of paging],
  ##    "nextPage": [url to next page depend on ulr],
  ##    "prevPage": [url to prvious page depend on url],
  ##    "numPage": [total number of page],
  ##    "numData": [total number of data],
  ##    "lastPage": [url to last page depend on url],
  ##    "firstPage": [url to first page depend on url],
  ##    "perPage": [number of displayed data for each page]
  ##  }
  ##
  
  let currentPage = url.getQuery("page", "1").parseBiggestInt
  result = %*{
    "pageData": data,
    "nextPage": "",
    "prevPage": "",
    "numPage": 1,
    "page": 1,
    "numData": numData,
    "lastPage": "",
    "firstPage": "",
    "perPage": perPage}
  
  if not data.isNil and data.kind == JsonNodeKind.JArray:
    if currentPage > 0:
      result["page"] = %currentPage

    if numData > perPage:
      let numPage = (numData.float64 / perPage.float64).ceil().int64
      result["numData"] = %numData
      result["numPage"] = %numPage
      if currentPage < numPage:
        url.setQuery("page", $(currentPage + 1))
        result["nextPage"] = % $url
        
      if currentPage <= numPage and currentPage > 1:
        url.setQuery("page", $(currentPage - 1))
        result["prevPage"] = % $url

      url.setQuery("page", $numPage)
      result["lastPage"] = % $url

      url.setQuery("page", "1")
      result["firstPage"] = % $url

proc genPagingLink*(
  url: string,
  pagingData: JsonNode,
  pagingStep: int64 = 10): JsonNode =
  ##
  ##  generate paging link:
  ##
  ##  this will be generate number of paging link to displayed.
  ##  pagingData parameter is result from genPaging.
  ##  for example we have 1000 data and want to display the link only 10 navigation per page.
  ##  the navigation link will updated if the data reach the pagingStep calculation.
  ##
  ##  return value in json format:
  ##  {
  ##    "pages": [pages data],
  ##    "numData": [total data],
  ##    "numPage": [total page],
  ##    "page": [current page],
  ##    "next": [next page url],
  ##    "prev": [prev page url],
  ##    "last": [last page url],
  ##    "first": [first page url]}
  ##  }
  ##
  var maxPageToShow = pagingStep
  let numPage = ($pagingData{"numPage"}).parseBiggestInt
  let page = ($pagingData{"page"}).parseBiggestInt
  result = %*{
    "pages": [],
    "numData": ($pagingData{"numData"}).parseBiggestInt,
    "numPage": numPage,
    "page": page,
    "next": "",
    "prev": "",
    "last": "",
    "first": ""}
  if numPage > 1:
    let pageUri = pagingData{"firstPage"}.getStr.parseUri3
    let firstPage = pagingData{"firstPage"}.getStr.parseUri3
    let lastPage = pagingData{"lastPage"}.getStr.parseUri3
    let nextPage = pagingData{"nextPage"}.getStr.parseUri3
    let prevPage = pagingData{"prevPage"}.getStr.parseUri3
    if maxPageToShow > numPage:
      maxPageToShow = numPage

    var startPage = (page div pagingStep) + 1
    var endPage = (startPage + maxPageToShow) - 1
    if endPage > numPage:
      endPage = numPage

    if numPage > pagingStep:
      if startPage != 1:
        result{"first"} = %(url & firstPage.getQueryString)

      if endPage != numPage:
        result{"last"} = %(url & lastPage.getQueryString)
      
      if prevPage.getPathSegments.len != 0:
        result{"prev"} = %(url & prevPage.getQueryString)
      
      if nextPage.getPathSegments.len != 0:
        result{"next"} = %(url & nextPage.getQueryString)
    
    for i in startPage..endPage:
      result{"pages"}.add(%*{
        "name": $i,
        "url": (url & pageUri.getQueryString)
          .replace("page=1", &"page={i}")})
