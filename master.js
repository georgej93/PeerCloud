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
var partitions = 0;

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
var activeWorkers = [];
var id_tracker = 0;
var data;

//Find idle worker and send partition
function findIdleWorker(callback) {
    var idle_worker_found = 0;
    var n = 0;
    console.log("Attempting to find idle worker");
    
    while(idle_worker_found == 0 && n<activeWorkers.length) {
        if(activeWorkers[n].worker_status === "idle") {
            idle_worker_found = 1;
            callback(activeWorkers[n].worker_id);
        }   
        n++;
    }
    
    //return 0;
    //console.log("No idle workers avilable to take task");   
}

function partitionData(err,data) {  
    var user_map = fs.readFileSync('./user_files/map.txt','utf8');
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
    //console.log("Read in " + totalLineCount + " lines of data");
    linesPerPartition = Math.ceil(totalLineCount / activeWorkers.length);
    //console.log("Lines per partition:",linesPerPartition);
    
    //Write lines to file as partitions
    lower = 0; upper = lower+linesPerPartition;
    while(lower+linesPerPartition < totalLineCount) {
        var lineList = split_data.slice(lower, upper);
        //console.log("Split from " + lower + " to " + upper + " resulted in: " + lineList);
        var lines = "";

        //Prepare lines string to write to file with newline control characters
        lineList.forEach(function(line) {
            lines += line + "\r\n";
        });
        
        //Create Partition
        fs.writeFile('./user_files/partitions/' + (partitions + 1) + '.txt', lines, function(err) {
            if(err) {
                return console.log("Error writing to file from",__dirname);
            }         
        });
        
        console.log("INCREMENTING PARITIONS FROM " + partitions + " TO " + (partitions + 1));
        partitions++; 
        distributePartition(partitions, user_map);
        
        lower += linesPerPartition;
        upper  = lower+linesPerPartition;
    }
    
    //Catch any missed lines due to rounding of lines per partition
    if(lower<totalLineCount) {
        var lineList = split_data.slice(lower,totalLineCount); 
        var lines = "";
        
        lineList.forEach(function(line) {
            lines += line + "\r\n";
        });
        
        fs.writeFile('./user_files/partitions/' + (partitions + 1) + '.txt', lines, function(err) {
            if(err) {
                return console.log("Error writing to file from",__dirname);
            }        
        });
        
        console.log("INCREMENTING PARITIONS FROM " + partitions + " TO " + (partitions + 1));
        partitions++; 
        distributePartition(partitions,user_map);
    }
    
    console.log("Done partitioning: generated " + partitions + " partitions.");
    
    var data1 = fs.readFile('./user_files/partitions/1.txt','utf8',checkData);
    var data2 = fs.readFile('./user_files/partitions/2.txt','utf8',checkData);
    var data3 = fs.readFile('./user_files/partitions/3.txt','utf8',checkData);

    //distributePartitions();
}

function checkData(err, data) {
    console.log("===============DATA READ TEST===============");
    console.log(data);
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

function distributePartition(partition_ref, user_map) {
    console.log("   DISTRIBUTE PARTITION CALLED WITH partition_ref", partition_ref);

    var send_obj = JSON.parse(JSON.stringify(user_map));
    console.log("       FLAG 1:",partition_ref);
                        
    findIdleWorker(function(idle_worker_id) {
        console.log("       FLAG 2:",partition_ref);
        console.log("IDLE WORKER:", idle_worker_id);
        
        activeWorkers.forEach(function(worker) {
            if(worker.worker_id == idle_worker_id) {
                activeWorkers[activeWorkers.indexOf(worker)].worker_status = "busy";                  
            }
        });
    
        io.of('/').clients((error, clients) => {
            console.log("Sending partition " + partition_ref + " to worker: " + idle_worker_id);
            io.sockets.connected[idle_worker_id].emit('TASK',partition_ref,send_obj);                    
        });
    
    });
  
}

server.listen(8080, function() {
    console.log('Master server listening on port 8080');
});

app.get('/', function(req, res) {
    //console.log('get route', req.testing);
    //res.end();
    res.sendFile(path.join(__dirname+'/public/index.html'));
});

app.get('/register-worker', function(req, res) {
    //Create and log a new worker: 0 parameter generates random free port
    //worker.createWorker(0);
    
    res.sendFile(path.join(__dirname+'/public/register_worker.html')); 
});

app.get('/worker-log', function(req, res) {
    //View pool of sockets connected to Master
    io.of('/').clients((error, clients) => {
        if(error) throw error;
        res.send(activeWorkers);
    });  
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
        console.log("Updating " + worker_id + " status to " + new_status + " from:");
        activeWorkers.forEach(function(worker) {
            if(worker.worker_id == socket.id) {
                console.log(activeWorkers[activeWorkers.indexOf(worker)].worker_status);
                activeWorkers[activeWorkers.indexOf(worker)].worker_status = new_status;
            }
        });
    });   
});

//TEST FUNCTIONS =========================
//What worker needs to recieve:
// -  map code to apply to contents of file
//What worker needs to return:
// - emit intermediate result
app.get('/file-test', function(req, res) {
    var filename = './user_files/input.txt';
    io.of('/').clients((error, clients) => {
        if(error) throw error;
        console.log("Server sending file: " + filename);
        //var fileObj = {"msg":"FILE" , ""}
        io.sockets.connected[clients[0]].emit('REQ_INFO');
    });
    res.redirect('/');
});

//Currently, filename is assumed to be locally stored and is collected through a text box on browser
//=> Will need to be a file upload
app.post('/send-filename', function(req,res) {
    //Lacks validation: invalid files crash page
    user_filename = './user_files/' + req.body.filename;
    res.redirect('/wordcount-test');
});

app.get('/wordcount-test', function(req,res) {   
    //clearData();

    //User uploaded file: input.txt
    //Distribution is performed in function called in partitionData
    console.log("Beginning to read user file");
    var filename = './user_files/input.txt';
    partitions = 0;
    data = fs.readFile(filename,'utf8',partitionData);
 
    res.redirect('/');
});

/*function distributePartitions() {
    //Send task to worker to perform
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
                //activeWorkers[activeWorkers.indexOf(idle_worker_id)].worker_status = "busy";
            
                //Set worker status to busy
                activeWorkers.forEach(function(worker) {
                    if(worker.worker_id == idle_worker_id) {
                        activeWorkers[activeWorkers.indexOf(worker)].worker_status = "busy";
                    }
                });
                
                console.log("Sending partition " + i + " to worker: " + idle_worker_id);
                io.sockets.connected[idle_worker_id].emit('TASK',i,obj);                  
            } else {
                console.log("No idle workers avilable to take task");
            }            
        }
    });
}*/