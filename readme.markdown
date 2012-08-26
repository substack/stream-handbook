# introduction

This document covers the basics of how to write [node.js](http://nodejs.org/)
programs with [streams](http://nodejs.org/docs/latest/api/stream.html).

```
"We should have some ways of connecting programs like garden hose--screw in
another segment when it becomes when it becomes necessary to massage data in
another way. This is the way of IO also."
```

[Doug McIlroy. October 11, 1964](http://cm.bell-labs.com/who/dmr/mdmpipe.html)

***

# basics

## readable

### pause / resume / drain

## writable

## pipe

## backpressure

## destroy

## stream-spec

***

# builtin streams

## fs.createReadStream()

## fs.createWriteStream()

## net.connect()

## net.createServer()

## http.request()

## http.createServer()

***

# control streams

## through

## from

## pause-stream

## concat-stream

## duplex

## duplexer

## mux-demux

## emit-stream

## invert-stream

## map-stream

## remote-events

## buffer-stream

## event-stream

## auth-stream

***

# state streams

## cdrt

## delta-stream

***

# io streams

## request

## reconnect

## kv

## discovery-network (relay stream)

***

# parser streams

## tar

## trumpet

## JSONStream

## json-scrape

## stream-serializer

***

# browser streams

## shoe

## domnode

## sorta

## graph-stream

## arrow-keys

## attribute

## data-bind

***

# rpc streams

## dnode

## rpc-stream

***

# test streams

## tap

## stream-spec

***

# stream abstractions

## stream-router

## multi-channel-mdm