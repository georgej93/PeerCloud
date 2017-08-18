var express = require('express');
var app = express();
var path = require('path');
var server = require('http').createServer(app);

server.listen(8081, function() {
    console.log('Master server listening on port 8081');
});

app.get('/', function(req, res) {
    res.sendFile(path.join(__dirname+'/public/index2.html'));
});