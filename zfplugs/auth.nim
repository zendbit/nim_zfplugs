import
  base64,
  strutils,
  httpcore

from zfcore import getHttpHeaderValues

proc validateBasicAuth*(httpHeaders: HttpHeaders, username: string, password: string): bool =
  let auth = httpHeaders.getHttpHeaderValues("Authorization").split(" ")
  if auth.len() == 2:
    let userPass = auth[1].decode().split(":")
    if userPass.len() == 2:
      result = userPass[0] == username and userPass[1] == password
