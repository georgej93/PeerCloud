
function createButton(name) {
    alert("createButton called for", name);
    var fileButton = document.createElement("INPUT");
    fileButton.setAttribute("type", "file");
    fileButton.setAttribute("multiple", false);
    fileButton.setAttribute("name", name);
    
    document.body.appendChild(fileButtonButton);
    
    alert("Created button:",name);
}

function createButtons() {
    alert("createButtons called");
    createButton("inputButton");
    createButton("mapButton");
    createButton("reduceButton");
    createButton("configButton");
}
