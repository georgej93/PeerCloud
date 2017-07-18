//To Do:
//X Output results to file: console capps out and gives a "... n more items" message for big fileSize
//X Place console.log messages that show with all certainty that the process is being performed asynchronously
//- Perhaps try running through the other IDE dan mentioned?
//- Program seems to work, but maybe try and get the formatData function only run once on the master?
//- Introduce a 2nd job to determine the consistent features of the framework1
//- Redo gantt chart
//- Maybe type up some stuff about the work so far

//Multiple files: master and worker
//Will require server file for communication
//Structure is not as clear: need to improve how the program functions
//Aim for individual threads / files.
//Cluster / forking not the mechanism to go forward with => move to separate files that communicate
//Options: HTTP communications between master / workers
//Each worker file is a server. Running multiple at once on differing ports.
//Master.js is also a server. Master needs to know the port branch of workers. Master needs to find this out
//HTTP: communication between URLs with IP addresses, ports, paths, domain names.
//Domain names: localhost:3000/<PATH>
//Path will be an endpoint to a function.
//In express, use handle ? to provide URLs
//Eg: /register-worker', function(request) {...}
//In request, have a worker map which contains all the ports and id's of each worker, or just a worker list.
//Worker list will be list of port names, but in future will be addresses of some sort (domain names / IP address / ports)
//Lots of these.
//Step 1: master running: everytime you start a new worker node, the first thing the worker does after the internal setup is register itself on your master. 
  //The master has got this known port address, so the worker will know how to identify the master. And the worker knows its own port, domain name etc. 
  //So it tells the master to register it, then go to sleep and just "listen" to events.
//The master has this pool of workers to draw from. In the worker, the express handle e.g /schedule-work
//Port will be part of URL, needs to be extracted
//When worker starts, do a get request on localhost:... with URL /register-worker
//Want to pass in the workers port for the master to log.
//Lots of different ways to send port. POST requests with data attributes (JSON objects) OR do a GET request in a RESTful way that has a URL such as localhost:3000/register=worker/port/3001
//In the register-worker URL would be able to get the port number. request.params['portnum'] to extracted
//Now the master can schedule work out amongst workers.
//WOrkers have defined function /schedule-work.
//From the request paramateres it would take filename, offset (?), filerange, whatever you need to describe your partition of data for the worker.
//From the parameters also get the map / reduce job. Maybe change url to /schedule-map-job
//So how can you communicate between those different processes. In same way that the user sends the map and reduce job to the master, the master can just take the same object, maybe it does some security check to it and then send it to the worker.
//The worker, has the file and can do .eval to evaluate a string as a piece of javascript. Look into eval.
//THen the work does job! Similar to the method in current version, abstracted. job(job, partition) etc
//Now we have result: in master, need another function master.handle(/read-result) with some request function handler. Go and read result.
//Also look up protocol buffers as an alternative to HTTP. Can then scrap HTTP stuff and use them instead. May make things easier.
//Important bit: needs to be clear for me. Then can evaluate if like using HTTP or want to use protocal buffers.

var fs = require('fs');
var cluster = require('cluster');
var mapreduce = require('mapred')(); // Leave blank for max performance
//var mapreduce = require('mapred')(1); // 1 = single core version (slowest)
//var mapreduce = require('mapred')(3); // Use a cluster of 3 processes

//Information to process =====================================================
var filename;    var wordcount;
var split_data;  var partitions;
var pieces = [];
var result = []; var maxWorkers = 5;
var workers = 2;

function formatData(data) {       
    //Remove control characters and punctuation to get "clean" data
    var clean_data = data.replace(/\r/g,"").replace(/\n/g," ").replace(/,/g,"").replace(/\./g,"").replace(/\?/g,"").replace(/\!/g,"");
    
    //Determine word allocation across the number of workers specified by maxWorkers
    var totalWordCount = getTotalWordCount(clean_data);
    var wordsPerPiece = Math.ceil(totalWordCount / maxWorkers);
    
    //Produce array of substrings by splitting on whitespace
    split_data = clean_data.split(' ');
    
    //Write words to file as partitions
    var lower = 0; var upper = lower+wordsPerPiece; partitions = 1;
    while(lower+wordsPerPiece < totalWordCount) {
        var wordList = split_data.slice(lower, upper);
        var words = "";
        
        wordList.forEach(function(word) {
            words += word + " ";
        });
        
        createPartition(words);      
        lower += wordsPerPiece;
        upper  = lower+wordsPerPiece;
    }

    //Prevent out of bounds when creating list of the last few words
    if(lower<totalWordCount) {
        var wordList = split_data.slice(lower,totalWordCount); 
        var words = "";
        
        wordList.forEach(function(word) {
            words += word + " ";
        });
        
        createPartition(words);
    }        
    
}

//Create a partition: data is written to a file with a numerical filename
function createPartition(data) {   
    fs.writeFile('./partitions/' + partitions + '.txt', data, function(err) {
        if(err) {
            return console.log("Error writing to file");
        }         
    });
    
    partitions++;
}

//FRAMEWORK Function: generalised MapReduce, where map and reduce are user specific
//User needs to provide:
// 1) WORK: the task that will be performed at each worker node
// 2) DATA: the data the work will be applied to
// E.g for wordocount: user supplies the text, and the work is the formatting / counting code
// But not all jobs may be split like a wordcount program: more needs to be user provided?

function getTotalWordCount(data) {
    var totalWordCount = 0;

    //Count whitespaces to determine wordcount
    for(i=0 ; i<data.length ; i++) {
        if(data[i] == ' ') {
            totalWordCount++;
        }
    }
    
    //Detect last word if text doesn't end in a whitespace
    if(data[data.length - 1] != ' ') {
        totalWordCount++;
    }
   
    return totalWordCount;
}

function countWords(data) {
   //Read data from partition and put into a list.
   var lower = 0, upper;
   var partition_list = [];
   
   //Count whitespaces to determine wordcount
    for(i=0 ; i<data.length ; i++) {
        if(data[i] == ' ') {
            upper = i;
            partition_list.push(data.slice(lower, upper));
            lower = upper;
        }
    }
    
    //Detect last word if text doesn't end in a whitespace
    /*if(data[data.length - 1] != ' ') {
        totalWordCount++;
    }*/
   
   console.log(partition_list);
   
   //Generate the word counts for all words within data
   //Counts is an associative array: the word is the index
   var list = [], counts = {};
   partition_list.forEach(function(word){
       //Comment out this line for output that shows the non asynchronous nature of workers
       //console.log("Worker " + workerId + " counted word: '" + word + "'");
       counts[word] = (counts[word] || 0) + 1;
   });
  
   partition_list.forEach(function (word) {
       console.log(word, "1");
   });
   
   //Combine the words and the counts together into a single list
   for(var k in counts){
       list.push([k, counts[k]]);
   } 

   console.log(list);
   return list;
   
}

function handlePartition(partitionNum) {
    console.log("in handlePartition(" + partitionNum + ")");
    var filename = './partitions/' + partitionNum + '.txt';
    fs.readFile(filename,'utf8', (err, data) => {
        console.log("read in partition_data: " + data);
        return countWords(data);
    });
}

function generatePartitions(callback) {
    //Attempt to read contents from file (blocking: not asynchronous)
    var contents = fs.readFileSync(filename,'utf8',formatData);

    if(contents == null) {
        console.log("Failed to read contents of file: " + filename);
        return console.log(err);
    } else {
        //Callback: format and split the contents
        callback(contents);       
    } 
}

//Handle a piece of data: handleFunction is the function to apply to the piece
function handlePiece(workerId, piece, handleFunction) {
    return handleFunction(workerId, piece);
}

function reduceResult(result) {
    var reducedResult = [];
    
    //Check each word against all others to find duplicate wordcounts.
    //Combine counts into one of them, then splice out the duplicate
    for(i=0 ; i<result.length ; i++) {
        for(j=0 ; j<result.length ; j++) {
            if((i!=j) && (result[i][0] === result[j][0])) {        
                result[i][1] += result[j][1];
                result.splice(j,1);
            }
        }
    }
    
    return result;
}

function resultToFile(result) {
    fs.writeFile('output.txt', "[Word , Count]" + '\r\n' + "--------------" + '\r\n', 'utf8');
    for(i=0 ; i<result.length ; i++) {
        fs.appendFile('output.txt', "["+ result[i][0] + " , " + result[i][1] + "]" + '\r\n', 'utf8');
    }
}

filename = process.argv[2];
fs.readFileSync(filename,'utf8',formatData);
handlePartition(1);

//fs.unlink('./partition1.txt');

/*if (process.argv.length > 3 || process.argv.length <= 2) {
    console.log("Too many / too few arguments: run program like 'node framework1.js input.txt'");
} else {
    filename = process.argv[2];
    generatePartitions(formatData);
    
    if(cluster.isMaster) {
        var finished = 0;
        
        //Create numer of workers specified by maxWorkers
        for(i=0 ; i<maxWorkers ; i++) {
            var worker = cluster.fork();
            console.log("Worker process created by Master process");
            
            //When worker informs master of work done, add the workers intermediate result to the result
            worker.on('message', function(msg) {
                if(msg.about == 'workFinished') {
                    result = result.concat(msg.intermediate);                
                }
            });
            
            //Check if a finishing worker is the last worker left: if so, fully reduce result
            worker.on('exit', function() {
                finished++;
                
                if(finished == maxWorkers) {
                    result = reduceResult(result);
                    resultToFile(result);
                    console.log("Process Finished: result written to file output.txt");
                }
            });
        }
    } else {
        //Create an intermediate result and return it to the Master
        var intermediateResult;
        if(pieces[cluster.worker.id - 1] != undefined) {
            intermediateResult = handlePiece([cluster.worker.id], pieces[cluster.worker.id - 1], countWords);
            
            console.log("Worker " + [cluster.worker.id] + " finished handling a piece");
            process.send({
                from: cluster.worker.id, 
                about: 'workFinished', 
                intermediate: intermediateResult
            });
        }
    
        cluster.worker.destroy();
    }/*

};

//PROMISES CODE:
//Create a promise that the data chunks will be created
//https://scotch.io/tutorials/javascript-promises-for-dummies
/*var promiseChunks = new Promise(function(resolve, reject) {
    if(chunksGenerated) {
        console.log("resolving promise");
        resolve(chunks);
    } else {
        var reason = new Error("Failed to generate data chunks");
        reject(reason);
    }      
});

//Proceed depending on status of promise
promiseChunks.then(function (fulfilled) {
    console.log("FULFILLED!");
    console.log(fulfilled);
    console.log("list1: " + chunks.pop());
    countWords(fulfilled.pop());
    countWords(fulfilled.pop());
})
.catch(function (error) {
    console.log(error.message);
});*/