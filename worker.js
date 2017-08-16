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
var currently_working = false;
var intermediates_written = 0;
var task_queue = [];

function generateInfo(id, stat) {
    var newWorker = {
        worker_id: id, 
        worker_status: stat,
        worker_rep: 1.0,
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
            currently_working = false;   
            socket.emit('PARTITION_UPDATE' , socket.id, partition_ref, "failure");
            socket.emit('REPUTATION_UPDATE', socket.id, "decrease"); 
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
        currently_working = false;   
        socket.emit('PARTITION_UPDATE', socket.id, partition_ref, "failure");
        socket.emit('REPUTATION_UPDATE', socket.id, "decrease"); 
        checkTaskQueue();
    } else {        
        //Create an intermediate file: named with worker_id. Worker should append if file already exists
        var intermediate_obj = { 'values' : intermediate_values};
        var writeable = JSON.stringify(intermediate_obj); 
        
        var intermediate_filename = './intermediate/' + socket.id + '-' + intermediates_written + '.txt';
        intermediates_written++;
        fs.writeFile(intermediate_filename, writeable, function(err) {
            socket.emit('STATUS_UPDATE', socket.id, "idle");
            currently_working = false;   
            if(err) {                         
                console.log(" ! ! ! ! - FAILURE 3 - ! ! ! !");               
                socket.emit('PARTITION_UPDATE', socket.id, partition_ref, "failure");        
                socket.emit('REPUTATION_UPDATE', socket.id, "decrease");               
            } else {               
                console.log("Intermediate file written:",intermediate_filename);
                socket.emit('PARTITION_UPDATE', socket.id, partition_ref, "done");
            }
            checkTaskQueue(socket);
        });
    }
    
}

function checkTaskQueue(socket) {
    let queue_length = task_queue.length;
    
    if(queue_length > 0) {
        performTask(task_queue[queue_length - 1].map_obj, socket, task_queue[queue_length - 1].partition_reference, emitIntermediateValues);
        task_queue.pop();
    }
}

//Create websocket connection
var socket = io.connect('http://localhost:8080/');

//Send worker info to server on request
socket.on('REQ_INFO', function(msg) {
    console.log(socket.id + " : Got a 'REQ_INFO' message from server");
    socket.emit('WORKER_INFO', generateInfo(socket.id, "idle"));
    currently_working = false;   
});  

//Perform test task
socket.on('TASK', function(partition_ref, obj) {
    //socket.emit('STATUS_UPDATE', socket.id, "busy");
    //Partition ref refers to an integer corresponding to a partitions filename
    console.log("Recieved task from server - partition:", partition_ref);
    
    //Testing: falisfying worker failures
    /*if(Math.floor(Math.random() * 2) == 1) {
        console.log("=========================== FABRICATED FAILURE =======================");
        socket.emit('PARTITION_UPDATE', socket.id, partition_ref, "done");
        socket.emit('REPUTATION_UPDATE', socket.id, "decrease");          
    } else {*/
    
        working_partition = './user_files/partitions/' + partition_ref + '.txt';
        if(task_queue.length > 0 || currently_working) {
            //Push new task to queue if there are existing tasks in queue or worker is doing something      
            let queued_task = {partition_reference : partition_ref,
                               map_obj : obj
                              }
            task_queue.push(queued_task);
        } else {
            currently_working = true;
            performTask(obj, socket, partition_ref, emitIntermediateValues);
        }
    
    //}
       
    
    //socket.emit('STATUS_UPDATE', socket.id, "idle");
    
});
