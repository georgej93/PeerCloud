//LINKS:
//https://www.npmjs.com/package/socket.io
//https://socket.io/get-started/chat/
//https://socket.io/docs/#
//https://github.com/socketio/socket.io/blob/7199d1b6ef812a13a4df09b5155b52f52517ea2d/docs/API.md#socketid
//https://github.com/socketio/socket.io-client/blob/master/docs/API.md#event-ping-1
//https://stackoverflow.com/questions/41737358/expressjs-and-socket-io-listening-to-different-ports-and-socket-io-client-connec
//https://stackoverflow.com/questions/24041220/sending-message-to-a-specific-id-in-socket-io-1-0
//Message recieving solution: https://stackoverflow.com/questions/26061335/express-with-socket-io-server-doesnt-receive-emits-from-client

//File Reading Variables:
var fs = require('fs');

var user_map; var user_reduce;
var partitions = 0; var completed = 0;
var dist_method;

var express = require('express');
var path = require('path');
var app = express();

//Body Parser & File Handling
var bodyParser = require('body-parser');
app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());
const fileUpload = require('express-fileupload');
app.use(fileUpload());

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
        createPartition(split_data, lower, totalLineCount, user_map);
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
                                     if(completed == partitions) {
                                        console.log(completed + "/" + partitions + " partitions returned, starting reduce ====================================");
                                        readIntermediateFiles(reduceResults);
                                     }
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

function reduceResults(results) {
    console.log("In reduce results with collected results:");
    //console.log(results);
    fs.readFile('./user_files/reduce.txt', 'utf8', function(err, data) {
        if(err) { 
            return console.log(err); 
        } else {
            var reduce = new Function('results', JSON.parse(JSON.stringify(data)));
            var reduced_results = reduce(results);            
        }
     
    });
    
}

function readIntermediateFiles(_callback) {
    var collected_results = [];
    var files_read = 0;
    
    fs.readdir('./intermediate',function(err, files) {
        if(err) { 
            return console.log(err); 
        } else {
            files.forEach(function(file) {
                fs.readFile('./intermediate/' + file, function(err, data) {
                   var obj = JSON.parse(data);
                   collected_results = collected_results.concat(obj.values);
                   files_read++;
                   if(files_read == completed) {
                       _callback(collected_results);
                   }
               });
            });           
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

app.post('/upload', function(req, res) {
    let config_mode = false;
    //Possible dist_methods: robin, random, weighted.
    dist_method = req.body.dist_method;
    console.log("dist_method set to:", dist_method);
    
    if(!req.files.data || !req.files.map || !req.files.reduce) {
        console.log("User input upload failed");
        res.send("Error: input data, map code or reduce code not provided");
    } else {
        let dataFile   = req.files.data;
        let mapFile    = req.files.map;
        let reduceFile = req.files.reduce;
        
        if(req.files.config) {
            config_mode = true;
            let configFile = req.files.config;
        }
        
        //Move files to user_files directory
        var path = './user_files/';    
        moveFile(dataFile, path + 'input.txt');
        moveFile(mapFile, path + 'map.txt');
        moveFile(reduceFile, path + 'reduce.txt');
        
        if(config_mode) {
            moveFile(configFile, path + 'config.txt');
        }
        console.log("User input was uploaded");
        res.redirect('/');
    }
});

function moveFile(file, path) {
    console.log("Moving file:", file.name);
    file.mv(path, function(err) {
        if(err) return(err);
    });
}

app.get('/submit', function(req, res) {
    clearData();
    
    //User uploaded file: input.txt
    //Distribution is performed in function called in partitionData
    console.log("Beginning to read user file");
    var filename = './user_files/input.txt';
    partitions = 0; completed = 0;
    data = fs.readFile(filename,'utf8',partitionData);
});

app.get('/wordcount-test', function(req,res) {   
    clearData();

    //User uploaded file: input.txt
    //Distribution is performed in function called in partitionData
    console.log("Beginning to read wordcount file");
    var filename = './user_files/wordcount.txt';
    partitions = 0; completed = 0;
    data = fs.readFile(filename,'utf8',partitionData);
 
    res.redirect('/');
});

app.get('/averaging-test', function(req,res) {   
    clearData();

    //User uploaded file: input.txt
    //Distribution is performed in function called in partitionData
    console.log("Beginning to read averaging file");
    var filename = './user_files/averaging.txt';
    partitions = 0; completed = 0;
    data = fs.readFile(filename,'utf8',partitionData);
 
    res.redirect('/');
});