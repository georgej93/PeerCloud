var fs = require('fs');

fs.readFile('unformatted.txt', 'utf8', function(err, data) {
    if(err) { return console.log(err); }
    
    var formatted_data = data.replace(/([.,*+?^=!:${}()|\"\'\;\-\[\]\/\\])/g,"").replace(/\s/g,'\r\n');;
    
    //var formatted_data = data.split(" ");
    
    fs.writeFile('formatted.txt', formatted_data, function(err) {
       if(err) { return console.log(err); } 
    });
    
});