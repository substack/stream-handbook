# introduction

This document covers the basics of how to write [node.js](http://nodejs.org/)
programs with [streams](http://nodejs.org/docs/latest/api/stream.html).

```
"We should have some ways of connecting programs like garden hose--screw in
another segment when it becomes necessary to massage data in
another way. This is the way of IO also."
```

[Doug McIlroy. October 11, 1964](http://cm.bell-labs.com/who/dmr/mdmpipe.html)

![doug mcilroy](http://substack.net/images/mcilroy.png)

***

Streams come to us from the
[earliest days of unix](http://www.youtube.com/watch?v=tc4ROCJYbm0)
and have proven themselves over the decades as a dependable way to compose large
systems out of small components that
[do one thing well](http://www.faqs.org/docs/artu/ch01s06.html).
In unix, streams are implemented by the shell with `|` pipes.
In node, the built-in
[stream module](http://nodejs.org/docs/latest/api/stream.html)
is used by the core libraries and can also be used by user-space modules.
Similar to unix, the node stream module's primary composition operator is called
`.pipe()` and you get a backpressure mechanism for free to throttle writes for
slow consumers.

Streams can help to
[separate your concerns](http://www.c2.com/cgi/wiki?SeparationOfConcerns)
because they restrict the implementation surface area into a consistent
interface that can be
[reused](http://www.faqs.org/docs/artu/ch01s06.html#id2877537).
You can then plug the output of one stream to the input of another and
[use libraries](http://npmjs.org) that operate abstractly on streams to
institute higher-level flow control.

Streams are an important component of
[small-program design](https://michaelochurch.wordpress.com/2012/08/15/what-is-spaghetti-code/)
and [unix philosophy](http://www.faqs.org/docs/artu/ch01s06.html)
but there are many other important abstractions worth considering.
Just remember that [technical debt](http://c2.com/cgi/wiki?TechnicalDebt)
is the enemy and to seek the best abstractions for the problem at hand.

![brian kernighan](http://substack.net/images/kernighan.png)

***

# why you should use streams

I/O in node is asynchronous, so interacting with the disk and network involves
passing callbacks to functions. You might be tempted to write code that serves
up a file from disk like this:

``` js
var http = require('http');
var fs = require('fs');

var server = http.createServer(function (req, res) {
    fs.readFile(__dirname + '/data.txt', function (err, data) {
        if (err) {
            res.statusCode = 500;
            res.end(String(err));
        }
        else res.end(data);
    });
});
server.listen(8000);
```

This code works but it's bulky and buffers up the entire `data.txt` file into
memory for every request before writing the result back to clients. If
`data.txt` is very large, your program could start eating a lot of memory as it
serves lots of users concurrently. The latency will also be high as users will
need to wait for the entire file to be read before they start receiving the
contents.

Luckily both of the `(req, res)` arguments are streams, which means we can write
this in a much better way using `fs.createReadStream()` instead of
`fs.readFile()`:

``` js
var http = require('http');
var fs = require('fs');

var server = http.createServer(function (req, res) {
    var stream = fs.createReadStream(__dirname + '/data.txt');
    stream.on('error', function (err) {
        res.statusCode = 500;
        res.end(String(err));
    });
    stream.pipe(res);
});
server.listen(8000);
```

Here `.pipe()` takes care of listening for `'data'` and `'end'` events from the
`fs.createReadStream()`. This code is not only cleaner, but now the `data.txt`
file will be written to clients one chunk at a time immediately as they are
received from the disk.

Using `.pipe()` has other benefits too, like handling backpressure automatically
so that node won't buffer chunks into memory needlessly when the remote client
is on a really slow or high-latency connection.

But this example, while much better than the first one, is still rather verbose.
The biggest benefit of streams is their versatility. We can
[use a module](https://npmjs.org/) that operates on streams to make that example
even simpler:

``` js
var http = require('http');
var filed = require('filed');

var server = http.createServer(function (req, res) {
    filed(__dirname + '/data.txt').pipe(res);
});
server.listen(8000);
```

With the [filed module](http://github.com/mikeal/filed) we get mime types, etag
caching, and error handling for free in addition to a nice streaming API.

Want compression? There are streaming modules for that too!

``` js
var http = require('http');
var filed = require('filed');
var oppressor = require('oppressor');

var server = http.createServer(function (req, res) {
    filed(__dirname + '/data.txt')
        .pipe(oppressor(req))
        .pipe(res)
    ;
});
server.listen(8000);
```

Now our file is compressed for browsers that support gzip or deflate! We can
just let [oppressor](https://github.com/substack/oppressor) handle all that
content-encoding stuff.

Once you learn the stream api, you can just snap together these streaming
modules like lego bricks or garden hoses instead of having to remember how to push
data through wonky non-streaming custom APIs.

Streams make programming in node simple, elegant, and composable.

# basics

Streams are just
[EventEmitters](http://nodejs.org/docs/latest/api/events.html#events_class_events_eventemitter)
that have a
[.pipe()](http://nodejs.org/docs/latest/api/stream.html#stream_stream_pipe_destination_options)
function and expected to act in a certain way depending if the stream is
readable, writable, or both (duplex).

To create a new stream, just do:

``` js
var Stream = require('stream');
var s = new Stream;
```

This new stream doesn't yet do anything because it is neither readable nor
writable.

## readable

To make that stream `s` into a readable stream, all we need to do is set the
`readable` property to true:

``` js
s.readable = true
```

Readable streams emit many `'data'` events and a single `'end'` event.
Your stream shouldn't emit any more `'data'` events after it emits the `'end'`
event.

This simple readable stream emits one `'data'` event per second for 5 seconds,
then it ends. The data is piped to stdout so we can watch the results as they

``` js
var Stream = require('stream');

function createStream () {
    var s = new Stream;
    s.readable = true

    var times = 0;
    var iv = setInterval(function () {
        s.emit('data', times + '\n');
        if (++times === 5) {
            s.emit('end');
            clearInterval(iv);
        }
    }, 1000);
    
    return s;
}

createStream().pipe(process.stdout);
```

```
substack : ~ $ node rs.js
0
1
2
3
4
substack : ~ $ 
```

In this example the `'data'` events have a string payload as the first argument.
Buffers and strings are the most common types of data to stream but it's
sometimes useful to emit other types of objects.

Just make sure that the types you're emitting as data is compatible with the
types that the writable stream you're piping into expects.
Otherwise you can pipe into an intermediary conversion or parsing stream before
piping to your intended destination.

## writable

Writable streams are streams that can accept input. To create a writable stream,
set the `writable` attribute to `true` and define `write()`, `end()`, and
`destroy()`.

This writable stream will count all the bytes from an input stream and print the
result on a clean `end()`. If the stream is destroyed it will do nothing.

``` js
var Stream = require('stream');
var s = new Stream;
s.writable = true;

var bytes = 0;

s.write = function (buf) {
    bytes += buf.length;
};

s.end = function (buf) {
    if (arguments.length) s.write(buf);
    
    s.writable = false;
    console.log(bytes + ' bytes written');
};

s.destroy = function () {
    s.writable = false;
};
```

If we pipe a file to this writable stream:

``` js
var fs = require('fs');
fs.createReadStream('/etc/passwd').pipe(s);
```

```
$ node writable.js
2447 bytes written
```

One thing to watch out for is the convention in node to treat `end(buf)` as a
`write(buf)` then an `end()`. If you skip this it could lead to confusion
because people expect end to behave the way it does in core.

## backpressure

Backpressure is the mechanism that streams use to make sure that readable
streams don't emit data faster than writable streams can consume data.

Note: the API for handling backpressure is changing substantially in future
versions of node (> 0.8). `pause()`, `resume()`, and `emit('drain')` are
scheduled for demolition. The notice has been on display in the local planning
office for months.

In order to do backpressure correctly readable streams should
implement `pause()` and `resume()`. Writable streams return `false` in
`.write()` when they want the readable streams piped into them to slow down and
emit `'drain'` when they're ready for more data again.

### writable stream backpressure

When a writable stream wants a readable stream to slow down it should return
`false` in its `.write()` function. This causes the `pause()` to be called on
each readable stream source.

When the writable stream is ready to start receiving data again, it should emit
the `'drain'` event. Emitting `'drain'` causes the `resume()` function to be
called on each readable stream source.

### readable stream backpressure

When `pause()` is called on a readable stream, it means that a downstream
writable stream wants the upstream to slow down. The readable stream that
`pause()` was called on should stop emitting data but that isn't always
possible.

When the downstream is ready for more data, the readable stream's `resume()`
function will be called.

## pipe

`.pipe()` is the glue that shuffles data from readable streams into writable
streams and handles backpressure. The pipe api is just:

```
src.pipe(dst)
```

for a readable stream `src` and a writable stream `dst`. `.pipe()` returns the
`dst` so if `dst` is also a readable stream, you can chain `.pipe()` calls
together like:

``` js
a.pipe(b).pipe(c).pipe(d)
```

which resembles what you might do in the shell with the `|` operator:

```
a | b | c | d
```

The `a.pipe(b).pipe(c).pipe(d)` usage is the same as:

```
a.pipe(b);
b.pipe(c);
c.pipe(d);
```

The stream implementation in core is just an event emitter with a pipe function.
`pipe()` is pretty short. You should read
[the source code](https://github.com/joyent/node/blob/master/lib/stream.js).

`.pipe(target)` returns the destination stream, `target`.
This means you can chain `.pipe()` calls together like in the shell with `|`.

## terms

These terms are useful for talking about streams.

### through

Through streams are simple readable/writable filters that transform input and
produce output.

### duplex

Duplex streams are readable/writable and both ends of the stream engage
in a two-way interaction, sending back and forth messages like a telephone. An
rpc exchange is a good example of a duplex stream. Any time you see something
like:

``` js
a.pipe(b).pipe(a)
```

you're probably dealing with a duplex stream.

## read more

* [core stream documentation](http://nodejs.org/docs/latest/api/stream.html#stream_stream)
* [notes on the stream api](http://maxogden.com/node-streams)
* [why streams are awesome](http://blog.dump.ly/post/19819897856/why-node-js-streams-are-awesome)

## the future

A big upgrade is planned for the stream api in node 0.9.
The basic apis with `.pipe()` will be the same, only the internals are going to
be different. The new api will also be backwards compatible with the existing
api documented here for a long time.

You can check the
[readable-stream](https://github.com/isaacs/readable-stream) repo to see what
these future streams will look like.

***

# built-in streams

These streams are built into node itself.

## process

### [process.stdin](http://nodejs.org/docs/latest/api/process.html#process_process_stdin)

This readable stream contains the standard system input stream for your program.

It is paused by default but the first time you refer to it `.resume()` will be
called implicitly on the
[next tick](http://nodejs.org/docs/latest/api/process.html#process_process_nexttick_callback).

If process.stdin is a tty (check with
[`tty.isatty()`](http://nodejs.org/docs/latest/api/tty.html#tty_tty_isatty_fd))
then input events will be line-buffered. You can turn off line-buffering by
calling `process.stdin.setRawMode(true)` BUT the default handlers for key
combinations such as `^C` and `^D` will be removed.

### [process.stdout](http://nodejs.org/api/process.html#process_process_stdout)

This writable stream contains the standard system output stream for your program.

`write` to it if you want send data to stdout

### [process.stderr](http://nodejs.org/api/process.html#process_process_stderr)

This writable stream contains the standard system error stream for your program.

`write` to it if you want send data to stderr

## child_process.spawn()

## fs

### fs.createReadStream()

### fs.createWriteStream()

## net

### [net.connect()](http://nodejs.org/docs/latest/api/net.html#net_net_connect_options_connectionlistener)

This function returns a [duplex stream] that connects over tcp to a remote
host.

You can start writing to the stream right away and the writes will be buffered
until the `'connect'` event fires.

### net.createServer()

## http

### http.request()

### http.createServer()

## zlib

### zlib.createGzip()

### zlib.createGunzip()

### zlib.createDeflate()

### zlib.createInflate()

***

# control streams

## through

## from

## pause-stream

## concat-stream

## duplex

## duplexer

## emit-stream

## invert-stream

## map-stream

## remote-events

## buffer-stream

## event-stream

## auth-stream

***

# meta streams

## mux-demux

## stream-router

## multi-channel-mdm

***

# state streams

## cdrt

## delta-stream

## [scuttlebutt](https://github.com/dominictarr/scuttlebutt)

[scuttlebutt](https://github.com/dominictarr/scuttlebutt) can be used for
peer-to-peer state synchronization with a mesh topology where nodes might only
be connected through intermediaries and there is no node with an authoritative
version of all the data.

The kind of distributed peer-to-peer network that
[scuttlebutt](https://github.com/dominictarr/scuttlebutt) provides is especially
useful when nodes on different sides of network barriers need to share and
update the same state. An example of this kind of network might be browser
clients that send messages through an http server to each other and backend
processes that the browsers can't directly connect to. Another use-case might be
systems that span internal networks since IPv4 addresses are scarce.

[scuttlebutt](https://github.com/dominictarr/scuttlebutt) uses a
[gossip protocol](https://en.wikipedia.org/wiki/Gossip_protocol)
to pass messages between connected nodes so that state across all the nodes will
[eventually converge](https://en.wikipedia.org/wiki/Eventual_consistency)
on the same value everywhere.

Using the `scuttlebutt/model` interface, we can create some nodes and pipe them
to each other to create whichever sort of network we want:

``` js
var Model = require('scuttlebutt/model');
var am = new Model;
var as = am.createStream();

var bm = new Model;
var bs = bm.createStream();

var cm = new Model;
var cs = cm.createStream();

var dm = new Model;
var ds = dm.createStream();

var em = new Model;
var es = em.createStream();

as.pipe(bs).pipe(as);
bs.pipe(cs).pipe(bs);
bs.pipe(ds).pipe(bs);
ds.pipe(es).pipe(ds);

em.on('update', function (key, value, source) {
    console.log(key + ' => ' + value + ' from ' + source);
});

am.set('x', 555);
```

The network we've created is an undirected graph that looks like:

```
a <-> b <-> c
      ^
      |
      v
      d <-> e
```

Note that nodes `a` and `e` aren't directly connected, but when we run this
script:

```
$ node model.js
x => 555 from 1347857300518
```

the value that node `a` set finds its way to node `e` by way of nodes `b` and
`d`. Here all the nodes are in the same process but because
[scuttlebutt](https://github.com/dominictarr/scuttlebutt) uses a
simple streaming interface, the nodes can be placed on any process or server and
connected with any streaming transport that can handle string data.

Next we can make a more realistic example that connects over the network and
increments a counter variable.

Here's the server which will set the initial `count` value to 0 and `count ++`
every 320 milliseconds, printing all updates to count:

``` js
var Model = require('scuttlebutt/model');
var net = require('net');

var m = new Model;
m.set('count', '0');
m.on('update', function (key, value) {
    console.log(key + ' = ' + m.get('count'));
});

var server = net.createServer(function (stream) {
    stream.pipe(m.createStream()).pipe(stream);
});
server.listen(8888);

setInterval(function () {
    m.set('count', Number(m.get('count')) + 1);
}, 320);
```

Now we can make a client that connects to this server, updates the count on an
interval, and prints all the updates it receives:

``` js
var Model = require('scuttlebutt/model');
var net = require('net');

var m = new Model;
var s = m.createStream();

s.pipe(net.connect(8888, 'localhost')).pipe(s);

m.on('update', function cb (key) {
    // wait until we've gotten at least one count value from the network
    if (key !== 'count') return;
    m.removeListener('update', cb);
    
    setInterval(function () {
        m.set('count', Number(m.get('count')) + 1);
    }, 100);
});

m.on('update', function (key, value) {
    console.log(key + ' = ' + value);
});
```

The client is slightly trickier since it should wait until it has an update from
somebody else to start updating the counter itself or else its counter would be
zeroed.

Once we get the server and some clients running we should see a sequence like this:

```
count = 183
count = 184
count = 185
count = 186
count = 187
count = 188
count = 189
```

Occasionally one some of the nodes we might see a sequence with repeated values like:

```
count = 147
count = 148
count = 149
count = 149
count = 150
count = 151
```

These values are due to
[scuttlebutt's](https://github.com/dominictarr/scuttlebutt)
history-based conflict resolution algorithm which is hard at work ensuring that the state of the system across all nodes is eventually consistent.

Note that the server in this example is just another node with the same
privledges as the clients connected to it. The terms "client" and "server" here
don't affect how the state synchronization proceeds, just who initiates the
connection. Protocols with this property are often called symmetric protocols.
See [dnode](https://github.com/substack/dnode) for another example of a
symmetric protocol.

***

# http streams

## request

## filed

## oppressor

## response-stream

***

# io streams

## reconnect

## kv

## discovery-network

***

# parser streams

## tar

## trumpet

## [JSONStream](https://github.com/dominictarr/JSONStream)

Use this module to parse and stringify json data from streams.

If you need to pass a large json collection through a slow connection or you
have a json object that will populate slowly this module will let you parse data
incrementally as it arrives.

## json-scrape

## stream-serializer

***

# browser streams

## [shoe](https://github.com/substack/shoe)

## domnode

## sorta

## graph-stream

## arrow-keys

## attribute

## data-bind

***

# audio streams

## [baudio](https://github.com/substack/baudio)

# rpc streams

## [dnode](https://github.com/substack/dnode)

[dnode](https://github.com/substack/dnode) lets you call remote functions
through any kind of stream.

Here's a basic dnode server:

``` js
var dnode = require('dnode');
var net = require('net');

var server = net.createServer(function (c) {
    var d = dnode({
        transform : function (s, cb) {
            cb(s.replace(/[aeiou]{2,}/, 'oo').toUpperCase())
        }
    });
    c.pipe(d).pipe(c);
});

server.listen(5004);
```

then you can hack up a simple client that calls the server's `.transform()`
function:

``` js
var dnode = require('dnode');
var net = require('net');

var d = dnode();
d.on('remote', function (remote) {
    remote.transform('beep', function (s) {
        console.log('beep => ' + s);
        d.end();
    });
});

var c = net.connect(5004);
c.pipe(d).pipe(c);
```

Fire up the server, then when you run the client you should see:

```
$ node client.js
beep => BOOP
```

The client sent `'beep`' to the server's `transform()` function and the server
called the client's callback with the result, neat!

The craziness begins when you start to pass function arguments to stubbed
callbacks. Here's an updated version of the previous server with a multi-stage
callback passing dance:

``` js
var dnode = require('dnode');
var net = require('net');

var server = net.createServer(function (c) {
    var d = dnode({
        transform : function (s, cb) {
            cb(function (n, fn) {
                var oo = Array(n+1).join('o');
                fn(s.replace(/[aeiou]{2,}/, oo).toUpperCase());
            });
        }
    });
    c.pipe(d).pipe(c);
});

server.listen(5004);
```

Here's the updated client:

``` js
var dnode = require('dnode');
var net = require('net');

var d = dnode();
d.on('remote', function (remote) {
    remote.transform('beep', function (cb) {
        cb(10, function (s) {
            console.log('beep:10 => ' + s);
            d.end();
        });
    });
});

var c = net.connect(5004);
c.pipe(d).pipe(c);
```

After we spin up the server, when we run the client now we get:

```
$ node client.js
beep:10 => BOOOOOOOOOOP
```

It just works!™

The basic idea is that you just put functions in objects and you call them from
the other side of a stream and the functions will be stubbed out on the other
end to do a round-trip back to the side that had the original function in the
first place. The best thing is that when you pass functions to a stubbed
function as arguments, those functions get stubbed out on the *other* side!

This approach of stubbing function arguments recursively shall henceforth be
known as the "turtles all the way down" gambit. The return values of any of your
functions will be ignored and only enumerable properties on objects will be
sent, json-style.

It's turtles all the way down!

![turtles all the way](http://substack.net/images/all_the_way_down.png)

Since dnode works in node or on the browser over any stream it's easy to call
functions defined anywhere and especially useful when paired up with
[mux-demux](https://github.com/dominictarr/mux-demux) to multiplex an rpc stream
for control alongside some bulk data streams.

## rpc-stream

***

# test streams

## tap

## stream-spec

***

# power combos

## roll your own socket.io

We can build a socket.io-style event emitter api over streams using some of the
libraries mentioned earlier in this document.

First  we can use [shoe](http://github.com/substack/shoe)
to create a new websocket handler server-side and
[emit-stream](https://github.com/substack/emit-stream)
to turn an event emitter into a stream that emits objects.
The object stream can then be fed into
[JSONStream](https://github.com/dominictarr/JSONStream)
to serialize the objects and from there the serialized stream can be piped into
the remote browser.

``` js
var EventEmitter = require('events').EventEmitter;
var shoe = require('shoe');
var emitStream = require('emit-stream');
var JSONStream = require('JSONStream');

var sock = shoe(function (stream) {
    var ev = new EventEmitter;
    emitStream(ev)
        .pipe(JSONStream.stringify())
        .pipe(stream)
    ;
    ...
});
```

Inside the shoe callback we can emit events to the `ev` function.  Here we'll
just emit different kinds of events on intervals:

``` js
var intervals = [];

intervals.push(setInterval(function () {
    ev.emit('upper', 'abc');
}, 500));

intervals.push(setInterval(function () {
    ev.emit('lower', 'def');
}, 300));

stream.on('end', function () {
    intervals.forEach(clearInterval);
});
```

Finally the shoe instance just needs to be bound to an http server:

``` js
var http = require('http');
var server = http.createServer(require('ecstatic')(__dirname));
server.listen(8080);

sock.install(server, '/sock');
```

Meanwhile on the browser side of things just parse the json shoe stream and pass
the resulting object stream to `eventStream()`. `eventStream()` just returns an
event emitter that emits the server-side events:

``` js
var shoe = require('shoe');
var emitStream = require('emit-stream');
var JSONStream = require('JSONStream');

var parser = JSONStream.parse([true]);
var stream = parser.pipe(shoe('/sock')).pipe(parser);
var ev = emitStream(stream);

ev.on('lower', function (msg) {
    var div = document.createElement('div');
    div.textContent = msg.toLowerCase();
    document.body.appendChild(div);
});

ev.on('upper', function (msg) {
    var div = document.createElement('div');
    div.textContent = msg.toUpperCase();
    document.body.appendChild(div);
});
```

Use [browserify](https://github.com/substack/node-browserify) to build this
browser source code so that you can `require()` all these nifty modules
browser-side:

```
$ browserify main.js -o bundle.js
```

Then drop a `<script src="/bundle.js"></script>` into some html and open it up
in a browser to see server-side events streamed through to the browser side of
things.

With this streaming approach you can rely more on tiny reusable components that
only need to know how to talk streams. Instead of routing messages through a
global event system socket.io-style, you can focus more on breaking up your
application into tinier units of functionality that can do exactly one thing
well.

For instance you can trivially swap out JSONStream in this example for
[stream-serializer](https://github.com/dominictarr/stream-serializer)
to get a different take on serialization with a different set of tradeoffs.
You could bolt layers over top of shoe to handle
[reconnections](https://github.com/dominictarr/reconnect) or heartbeats
using simple streaming interfaces.
You could even add a stream into the chain to use namespaced events with
[eventemitter2](https://npmjs.org/package/eventemitter2) instead of the
EventEmitter in core.

If you want some different streams that act in different ways it would likewise
be pretty simple to run the shoe stream in this example through mux-demux to
create separate channels for each different kind of stream that you need.

As the requirements of your system evolve over time, you can swap out each of
these streaming pieces as necessary without as many of the all-or-nothing risks
that more opinionated framework approaches necessarily entail.
