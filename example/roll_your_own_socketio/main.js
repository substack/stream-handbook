var shoe = require('shoe');
var emitStream = require('emit-stream');
var JSONStream = require('JSONStream');

var parser = JSONStream.parse([true]);
var stream = shoe('/sock').pipe(parser);
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
