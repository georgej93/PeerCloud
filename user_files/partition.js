//PARITION.JS: formats text data in preparation for a wordcount procedure
//Formatting involves  : removing punctuation and new lines
//Partitioning involves: splitting file into numerous files based on number of workers

//Code Requirements: temporary to allow partition.js to run isolated (?)
var fs = require('fs');
var max_workers = 5;

//Function Parameters: to be passed in
var filename = user_filename; //name of file to be split into partitions
var data = "";                //alternatively, the data read in from file earlier 
    data = fs.readFileSync(filename,'utf8');

//Function Variables:
var totalWordCount; var wordsPerPartition;
var lower ; var upper;
var partitions = 0;

var partitionData = function() {
    //Remove control characters and punctuation to get formatted data
    data = data.replace(/\r/g,"").replace(/\n/g," ").replace(/,/g,"").replace(/\./g,"").replace(/\?/g,"").replace(/\!/g,"");

    //Determine word allocation across the number of workers specified by max_workers
    totalWordCount = 0;

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

    wordsPerPartition = Math.ceil(totalWordCount / max_workers);

    //Produce array of substrings by splitting on whitespace
    split_data = data.split(' ');

    //Write words to file as partitions
    lower = 0; upper = lower+wordsPerPartition;
    while(lower+wordsPerPartition < totalWordCount) {
        var wordList = split_data.slice(lower, upper);
        var words = "";
        
        wordList.forEach(function(word) {
            words += word + " ";
        });
        
        //Create Partition
        fs.writeFile('./user_files/partitions/' + (partitions + 1) + '.txt', words, function(err) {
            if(err) {
                return console.log("Error writing to file from",__dirname);
            }         
        });
        
        partitions++; 
        
        lower += wordsPerPartition;
        upper  = lower+wordsPerPartition;
    }

    //Prevent out of bounds when creating list of the last few words
    if(lower<totalWordCount) {
        var wordList = split_data.slice(lower,totalWordCount); 
        var words = "";
        
        wordList.forEach(function(word) {
            words += word + " ";
        });
        
        fs.writeFile('./user_files/partitions/' + (partitions + 1) + '.txt', words, function(err) {
            if(err) {
                return console.log("Error writing to file from",__dirname);
            }         
        });
        
        partitions++; 
    }

    return partitions;
}

partitionData();