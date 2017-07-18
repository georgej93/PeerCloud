//socket.io connection for new worker:
const io = require('socket.io-client');
var socket;

//File Reading Variables:
var fs = require('fs');

//Worker Info Variables:
var worker_info, id, port, host;
var working_partition;

function generateInfo(id, stat) {
    var newWorker = {
        worker_id: id, 
        worker_status: stat,
        worker_partition: 0
    };
    
    return newWorker;
}

function getIntermediateValues(jsonObj, socket, _callback) {
    console.log("in getIntVals");
    var intermediate_values = eval(jsonObj);
    _callback(intermediate_values, socket);
}

function emitIntermediateValues(intermediate_values,socket) {   
    /*intermediate_values.forEach(function(intermediate_value){
        socket.emit('INTERMEDIATE_VALUE', intermediate_value);
    });*/
    
    //Create an intermediate file: named with worker_id. Worker should append if file already exists
    var intermediate_obj = { "values" : intermediate_values};
    var writeable = JSON.stringify(intermediate_obj);
    
    fs.writeFile('./intermediate/' + socket.id + '.txt', writeable, function(err) {
        if(err) {
            console.log(err);
            return console.log("Error writing intermediate values to file");
        }         
    });
}

//Create websocket connection
var socket = io.connect('http://localhost:8080/');

//These will be browser windows, not servers in the browser.
//Do not need the server part above: everything done through websocket.
//Ready to be moved to web browser eventually.
//Need to get JS from somewhere when running in broswer: could be a file or a website that the master service is running
//Master.js = node.js process on port 8080
//Would be another website on different port, which contains a JS file, that browser downloads and JS file does everything got so far (worker nodes etc)

//Send worker info to server on request
socket.on('REQ_INFO', function(msg) {
    console.log(socket.id + " : Got a 'REQ_INFO' message from server");
    socket.emit('WORKER_INFO', generateInfo(socket.id, "idle"));
});  

//Parse test JSON object
socket.on('JSON', function(obj) {
    console.log("Recieved JSON object to parse: " + obj.code);
    eval(obj.code);
});

//Recieve test file
socket.on('FILE', function(fileObj) {
    console.log("Recieved File to read.");   
});

//Perform test task
socket.on('TASK', function(partition_ref, obj) {
    socket.emit('TASK_RECIEVED', socket.id);
    //Partition ref refers to an integer corresponding to a partitions filename
    working_partition = './user_files/partitions/' + partition_ref + '.txt';
    getIntermediateValues(obj, socket, emitIntermediateValues);
});
