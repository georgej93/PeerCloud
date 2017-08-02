//LINKS:
//https://www.npmjs.com/package/socket.io
//https://socket.io/get-started/chat/
//https://socket.io/docs/#
//https://github.com/socketio/socket.io/blob/7199d1b6ef812a13a4df09b5155b52f52517ea2d/docs/API.md#socketid
//https://github.com/socketio/socket.io-client/blob/master/docs/API.md#event-ping-1
//https://stackoverflow.com/questions/41737358/expressjs-and-socket-io-listening-to-different-ports-and-socket-io-client-connec
//https://stackoverflow.com/questions/24041220/sending-message-to-a-specific-id-in-socket-io-1-0
//Message recieving solution: https://stackoverflow.com/questions/26061335/express-with-socket-io-server-doesnt-receive-emits-from-client

var fs = require('fs');
var user_filename;
var user_map;
var partitions = 0;
var completed = 0;

var express = require('express');
var path = require('path');
var app = express();

//Body Parser
var bodyParser = require('body-parser');
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

//socket.io module requirements: https://www.npmjs.com/package/socket.io
var server = require('http').createServer(app);
var io = require('socket.io')(server);
const adminNamespace = io.of('/');

//book-keeping variables: redundant now? (websockets working?)
//not even using the port number of workers anymore? => using client id
var activeWorkers    = [];
var activePartitions = [];
var id_tracker = 0;
var data;

//Find idle worker and send partition
function findIdleWorker(callback) {
    var idle_worker_found = 0;
    var n = 0;
    console.log("Attempting to find idle worker from following workers:");
    console.log(activeWorkers);
    
    while(idle_worker_found == 0 && n<activeWorkers.length) {
        if(activeWorkers[n].worker_status === "idle") {
            idle_worker_found = 1;
            callback(activeWorkers[n].worker_id);
            //return activeWorkers[n].worker_id;
        }   
        n++;
    }
    
    //return 0;
    //console.log("No idle workers avilable to take task");   
}


var distributePartition = function (partition_ref, user_map) {
    console.log("   DISTRIBUTE PARTITION CALLED WITH partition_ref", partition_ref);

    var send_obj = JSON.parse(JSON.stringify(user_map));
                        
    findIdleWorker(function(idle_worker_id) {
        console.log("IDLE WORKER:", idle_worker_id);
        
        activeWorkers.forEach(function(worker) {
            if(worker.worker_id == idle_worker_id) {
                updateWorkerStatus(idle_worker_id, "busy");
            }
        });
    
        io.of('/').clients((error, clients) => {
            console.log("Sending partition " + partition_ref + " to worker: " + idle_worker_id);
            io.sockets.connected[idle_worker_id].emit('TASK',partition_ref,send_obj);  
            activePartitions.push(generatePartitionTracker(idle_worker_id,"handling",partition_ref));
        });
    
    });
  
}

function partitionData(err, data) {  
    user_map = fs.readFileSync('./user_files/map.txt','utf8');
    //console.log("In partition data with data:");
    //console.log(data);
    
    if(err){
        return console.log("Data partition failed:",err);
    }
    
    //console.log("Partitioning data based on " + activeWorkers.length + " workers.");
    
    var totalLineCount; var linesPerPartition;
    var lower; var upper;
    var split_data;
    
    //Remove control characters to get formatted data
    data = data.replace(/\r/g,"");
    
    //Produce array of substrings by splitting on newlines
    split_data = data.split("\n");
    
    //Determine word allocation across the number of workers connected
    totalLineCount = split_data.length;
    linesPerPartition = Math.ceil(totalLineCount / activeWorkers.length);

    //Write lines to file as partitions
    lower = 0; upper = lower+linesPerPartition;
    while(lower+linesPerPartition < totalLineCount) {
        createPartition(split_data, lower, upper, user_map);
        
        lower += linesPerPartition;
        upper  = lower+linesPerPartition;
    }
    
    //Catch any missed lines due to rounding of lines per partition
    if(lower<totalLineCount) {
        createPartition(split_data, lower, totalLineCount, user_map, distributePartition);
    }
    
    console.log("Done partitioning: generated " + partitions + " partitions.");
}

//Callback here is the distribute function
function createPartition(split_data, lower, upper, user_map) {
        var lineList = split_data.slice(lower, upper);
        var lines = "";

        //Prepare lines string to write to file with newline control characters
        lineList.forEach(function(line) {
            lines += line + "\r\n";
        });
        
        //Create Partition
        var partition_ref = partitions + 1;
        fs.writeFile('./user_files/partitions/' + partition_ref + '.txt', lines, function(err) {
            if(err) {
                return console.log("Error writing to file from",__dirname);
            }      
            distributePartition(partition_ref, user_map);
        });
        
        partitions++; 
}

function clearData() {
    //Clear any pre-existing files:
    var partitionFiles    = fs.readdirSync('./user_files/partitions');
    var intermediateFiles = fs.readdirSync('./intermediate');
    
    partitionFiles.forEach(function(file) {
        fs.unlinkSync('./user_files/partitions/' + file);
    });
    
    console.log("**** ALL OLD PARTITIONS CLEARED ****");
    
    intermediateFiles.forEach(function(file) {
        fs.unlinkSync('./intermediate/' + file);
    });
    
    console.log("**** ALL OLD INTERMEDIATES CLEARED ****");
    
}

function updateWorkerStatus(worker_id, new_status) {
    console.log("Updating " + worker_id + " status to " + new_status + " from:");
    activeWorkers.forEach(function(worker) {
        if(worker.worker_id == worker_id) {
            console.log(activeWorkers[activeWorkers.indexOf(worker)].worker_status);
            activeWorkers[activeWorkers.indexOf(worker)].worker_status = new_status;                  
        }
    });
}

function updatePartitionTracker(sender_id, partition_reference, new_status) {
    activePartitions.forEach(function(partition) {
        if(partition.recipient_id == sender_id) {
            if(partition.partition_ref == partition_reference) {
                switch(new_status) {
                    //Partition completed: keep running total of completed and remove from tracker
                    case 'done'    : console.log("Partition " + partition_reference + " returned sucessfully");
                                     completed++;
                                     activePartitions.splice(activePartitions.indexOf(partition), 1);
                                     break;
                    //Attempt to redistribute any failed partitions
                    case 'failure' : distributePartition(partition_reference, user_map);
                                     break;
                }
                partition.partition_status = new_status;
            }
        }
    });

}

function generatePartitionTracker(id, stat, part_ref) {
    var newPartition = {
        recipient_id: id, 
        partition_status: stat,
        partition_ref: part_ref
    };  
    
    return newPartition;
}

server.listen(8080, function() {
    console.log('Master server listening on port 8080');
});

app.get('/', function(req, res) {
    //console.log('get route', req.testing);
    //res.end();
    res.sendFile(path.join(__dirname+'/public/index.html'));
});

app.get('/worker-tracker', function(req, res) {
    //View pool of sockets connected to Master
    io.of('/').clients((error, clients) => {
        if(error) throw error;
        res.send(activeWorkers);
    });  
});

app.get('/partition-tracker', function(req, res) {
    //View pool of sockets connected to Master
    res.send(activePartitions);
});

//WORKER EVENTS:
io.on('connect', (socket) => {
   console.log("New worker connected with id : " + socket.id);
   io.of('/').clients((error, clients) => {
        if(error) throw error;
        //Message new worker to request worker info
        io.sockets.connected[socket.id].emit('REQ_INFO');
    });
    
    //Store Worker Info
    socket.on('WORKER_INFO', function (new_worker) {
        console.log("Recieved worker info, adding to tracker");
        activeWorkers.push(new_worker);
    });
    
    //Remove Disconnected Workers
    socket.on('disconnect', function() {
        console.log("Worker disconnected with id : " + socket.id);
        activeWorkers.forEach(function(worker) {
            if(worker.worker_id == socket.id) {
                activeWorkers.splice(activeWorkers.indexOf(worker), 1);
            }
        });
    });
    
    socket.on('INTERMEDIATE_VALUE', function(val) {
        //DO A REDUCE!
        console.log("Recieved an intermediate value", val);
    });
    
    //Update Worker Status
    socket.on('STATUS_UPDATE', function(worker_id, new_status) {
        updateWorkerStatus(worker_id, new_status);
    });   
    
    //Update partition tracker
    socket.on('PARTITION_UPDATE', function(worker_id, partition_ref, partition_status) {
        updatePartitionTracker(worker_id, partition_ref, partition_status);
    });
    
});

//Currently, filename is assumed to be locally stored and is collected through a text box on browser
//=> Will need to be a file upload
app.post('/send-filename', function(req,res) {
    //Lacks validation: invalid files crash page
    user_filename = './user_files/' + req.body.filename;
    res.redirect('/wordcount-test');
});

app.get('/wordcount-test', function(req,res) {   
    clearData();

    //User uploaded file: input.txt
    //Distribution is performed in function called in partitionData
    console.log("Beginning to read user file");
    var filename = './user_files/input.txt';
    partitions = 0; 
    var err = '';
    data = fs.readFile(filename,'utf8',partitionData);
 
    /*partitions = 5;
    //Send tasks to workers to perform
    var filename = './user_files/map.txt';
    var toSend = fs.readFileSync(filename,'utf8');
    var obj = JSON.parse(JSON.stringify(toSend));

    io.of('/').clients((error, clients) => {
        if(error) throw error;
        
        if(partitions==0) {
            return console.log("Failed to allocate partitions: no partitions created");
        }
        
        
        //Distribute all partitions amongst workers if possible
        for(i=1 ; i<=partitions ; i++) {
            //console.log(activeWorkers);
            
            var idle_worker_id = findIdleWorker();

            if(idle_worker_id != 0) {
                console.log("IDLE WORKER:", idle_worker_id);
            
                //Set worker status to busy
                activeWorkers.forEach(function(worker) {
                    if(worker.worker_id == idle_worker_id) {
                        updateWorkerStatus(idle_worker_id, "busy");
                    }
                });
                
                console.log("Sending partition " + i + " to worker: " + idle_worker_id);
                io.sockets.connected[idle_worker_id].emit('TASK',i,obj);                  
            } else {
                console.log("No idle workers avilable to take task");
            }            
        }
    });*/
    
    res.redirect('/');
});