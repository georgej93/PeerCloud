//socket.io connection for new worker:
const io = require('socket.io-client');
var socket;

//Testing Variables
var tests = 0;
var failures = 0;

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

function performTask(jsonObj, socket, partition_ref, _callback) { 
    var map = new Function('data', jsonObj);
    fs.readFile('./user_files/partitions/' + partition_ref + '.txt', 'utf8', function(err,data) {
        //Test data failure:
        //data = "";
        
        //Emit failure if no data read in
        if(!data) {
            console.log(" ! ! ! ! - FAILURE 1 - ! ! ! !");
            socket.emit('STATUS_UPDATE', socket.id, "idle");
            socket.emit('PARTITION_UPDATE', socket.id, partition_ref, "failure");
        } else {
            var intermediate_values = map(data);
            _callback(intermediate_values, socket, partition_ref);          
        }        
    });
    
};

function emitIntermediateValues(intermediate_values, socket, partition_ref) {   
    //In the case of a worker producing no results, consider this as a failed job
    //console.log(intermediate_values);
    if(intermediate_values.length == 0) {
        console.log(" ! ! ! ! - FAILURE 2 - ! ! ! !");
        socket.emit('STATUS_UPDATE', socket.id, "idle");
        socket.emit('PARTITION_UPDATE', socket.id, partition_ref, "failure");
    } else {        
        //Create an intermediate file: named with worker_id. Worker should append if file already exists
        var intermediate_obj = { 'values' : intermediate_values};
        var writeable = JSON.stringify(intermediate_obj); 
        
        console.log(writeable);
        
        fs.writeFile('./intermediate/' + socket.id + '.txt', writeable, function(err) {
            if(err) {
                console.log(" ! ! ! ! - FAILURE 3 - ! ! ! !");
                socket.emit('STATUS_UPDATE', socket.id, "idle");
                socket.emit('PARTITION_UPDATE', socket.id, partition_ref, "failure");
            } else {
                socket.emit('STATUS_UPDATE', socket.id, "idle");
                socket.emit('PARTITION_UPDATE', socket.id, partition_ref, "done");
            }
        });
    }
    
}

//Create websocket connection
var socket = io.connect('http://localhost:8080/');

//Send worker info to server on request
socket.on('REQ_INFO', function(msg) {
    console.log(socket.id + " : Got a 'REQ_INFO' message from server");
    socket.emit('WORKER_INFO', generateInfo(socket.id, "idle"));
});  

//Perform test task
socket.on('TASK', function(partition_ref, obj) {
    //socket.emit('STATUS_UPDATE', socket.id, "busy");
    console.log("Recieved task from server - partition:", partition_ref);
    
    //Partition ref refers to an integer corresponding to a partitions filename
    working_partition = './user_files/partitions/' + partition_ref + '.txt';
    //console.log("Got data:");
    /*fs.readFile(working_partition, 'utf8', function(err, data) {
        tests++;
        if(data.length < 1) {
            failures++;
        }
        console.log(failures + "/" + tests + " tests have failed");
    });*/
    
    
    performTask(obj, socket, partition_ref, emitIntermediateValues);
    //socket.emit('STATUS_UPDATE', socket.id, "idle");
    
});
