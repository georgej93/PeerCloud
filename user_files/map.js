//MAP.JS: reads data from partitions and performs a wordcount

//Code Requirements: temporary to allow partition.js to run isolated (?)
var fs = require('fs');
var working_partition = './user_files/partitions/1.txt';

//Read data from partition and put into a list.
var lower = 0, upper;
var partition_list = [];

var data = fs.readFileSync(working_partition,'utf8');

//Add words in partition to a list based on whitespaces
for(i=0 ; i<data.length ; i++) {
    if(data[i] == ' ') {
        upper = i;
        partition_list.push(data.slice(lower, upper));
        lower = upper;
    }
}

//Generate the word counts for all words within data
//Counts is an associative array: the word is the index
var list = [], counts = {};
partition_list.forEach(function(word){
   counts[word] = (counts[word] || 0) + 1;
});

//Combine the words and the counts together into a single list to return
for(var k in counts){
   list.push([k, counts[k]]);
} 

console.log("Returning a list");
return list;

