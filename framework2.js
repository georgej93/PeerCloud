//To Do:
//X Output results to file: console capps out and gives a "... n more items" message for big fileSize
//X Place console.log messages that show with all certainty that the process is being performed asynchronously
//- Perhaps try running through the other IDE dan mentioned?
//- Program seems to work, but maybe try and get the formatData function only run once on the master?
//- Introduce a 2nd job to determine the consistent features of the framework1
//- Redo gantt chart
//- Maybe type up some stuff about the work so far

var fs = require('fs');
var cluster = require('cluster');
var Jimp = require('jimp');
var mapreduce = require('mapred')(); // Leave blank for max performance
//var mapreduce = require('mapred')(1); // 1 = single core version (slowest)
//var mapreduce = require('mapred')(3); // Use a cluster of 3 processes

var filename;
var split_data;  var pieceDistribution = []; 
var pieces = []; var piecesGenerated = 0;
var result = []; var maxWorkers = 5;

function getImageData(data) {    
    var rows = [];

    //Create a list for each worker, containing pieces to be handled
    for(i=0 ; i<maxWorkers ; i++) {
        var pieceList = [];
        pieceDistribution.push(pieceList);       
    }

    //Collect Image
    Jimp.read(filename, function(err, userImage) {
        if(err) { console.log("Unable to read image"); throw(err); }
        else    { console.log("Loaded image..."); }

        //Image Properties
        var imageHeight = userImage.bitmap.height; 
        var imageWidth  = userImage.bitmap.width;
                           
        //Store all pixel values in a list as "rows"
        for(i=0 ; i<imageHeight; i++) {
            var list = [];
            for(j=0 ; j<imageWidth; j++) {
                rows.push(Jimp.intToRGBA(userImage.getPixelColor(i,j)));
            } 
        }
        
        //console.log(rows);
        
        //Assign rows to workers in a round-robin distribution style
        assignRoundRobin(rows, maxWorkers);
    });
}

//Round Robin: distributes all pieces across all workers as evenly as possible
//Data   : list containing pieces of data to be assigned
//Workers: number of workers to spread load across
function assignRoundRobin(data, workers) {
    var pieces_allocated = 0;
    var worker_id = 0;
    while(pieces_allocated < data.length) {
        pieceDistribution[worker_id].push(data[pieces_allocated]);
        pieces_allocated++;
        worker_id++;
        
        if(worker_id > maxWorkers - 1) {
            worker_id = 0;
        }
    }  
}

function averagePixelValues(workerId, data) {
    var averaged_values = {r: 0 , g: 0 , b: 0 , a: 0};
    var total_r = 0, total_g = 0, total_b = 0, total_a = 0;
      
    console.log(data[0].r);  
    
    for(i=0 ; i<=data.length - 1; i++) {
        total_r += data[i].r; total_g += data[i].g;
        total_b += data[i].b; total_a += data[i].a;
    }
    
    averaged_values.r = Math.ceil(total_r / data.length);
    averaged_values.g = Math.ceil(total_g / data.length);
    averaged_values.b = Math.ceil(total_b / data.length);
    averaged_values.a = Math.ceil(total_a / data.length);
    
    console.log(averaged_values);
    
    return averaged_values;
}

//Handle a piece of data: handleFunction is the function to apply to the piece
function handlePiece(workerId, pieces, handleFunction) {
    return handleFunction(workerId, pieces);
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

//The getting data part is really the first step of "mapping", where mapping is broken
//down into: 1) extracting the data from source per the users code, 
//           2) assigning it to be computed when worker is created

if (process.argv.length > 3 || process.argv.length <= 2) {
    console.log("Too many / too few arguments: run program like 'node framework1.js 'image.jpg'");
} else {
    filename = process.argv[2];
    //Promise requied?: no longer handling a file in a forced synchronized fashion
    getImageData();
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