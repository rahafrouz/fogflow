'use strict';

$(function(){

// initialize the menu bar
var handlers = {}

var CurrentScene = null;

// icon image for device registration
var iconImage = null;
var iconImageFileName = null;
// content image for camera devices
var contentImage = null;
var contentImageFileName = null;

// the list of all registered operators
var operatorList = [];

// design board
var blocks = null;

// client to interact with IoT Broker
var client = new NGSI10Client(config.brokerURL);

var myFogFunctionExamples = [
{
    topology: {"name":"anomaly-detection","description":"detect anomaly events in shops","tasks":[{"name":"Counting","operator":"counter","input_streams":[{"selected_type":"Anomaly","selected_attributes":[],"groupby":"ALL","scoped":true}],"output_streams":[{"entity_type":"Stat"}]},{"name":"Detector","operator":"anomaly","input_streams":[{"selected_type":"PowerPanel","selected_attributes":[],"groupby":"EntityID","scoped":true},{"selected_type":"Rule","selected_attributes":[],"groupby":"ALL","scoped":false}],"output_streams":[{"entity_type":"Anomaly"}]}]},
    intent: {},
    designboard: {"edges":[{"id":2,"block1":3,"connector1":["stream","output"],"block2":1,"connector2":["streams","input"]},{"id":3,"block1":2,"connector1":["outputs","output",0],"block2":3,"connector2":["in","input"]},{"id":4,"block1":4,"connector1":["stream","output"],"block2":2,"connector2":["streams","input"]},{"id":5,"block1":5,"connector1":["stream","output"],"block2":2,"connector2":["streams","input"]}],"blocks":[{"id":1,"x":202,"y":-146,"type":"Task","module":null,"values":{"name":"Counting","operator":"counter","outputs":["Stat"]}},{"id":2,"x":-194,"y":-134,"type":"Task","module":null,"values":{"name":"Detector","operator":"anomaly","outputs":["Anomaly"]}},{"id":3,"x":4,"y":-18,"type":"Shuffle","module":null,"values":{"selectedattributes":["all"],"groupby":"ALL"}},{"id":4,"x":-447,"y":-179,"type":"EntityStream","module":null,"values":{"selectedtype":"PowerPanel","selectedattributes":["all"],"groupby":"EntityID","scoped":true}},{"id":5,"x":-438,"y":-5,"type":"EntityStream","module":null,"values":{"selectedtype":"Rule","selectedattributes":["all"],"groupby":"ALL","scoped":false}}]}
}
];

addMenuItem('FogFunction', showFogFunctions);         
addMenuItem('TaskInstance', showTaskInstances);        

showFogFunctions();

queryOperatorList();

queryFogFunctions();


$(window).on('hashchange', function() {
    var hash = window.location.hash;
		
    selectMenuItem(location.hash.substring(1));
});

function addMenuItem(name, func) {
    handlers[name] = func; 
    $('#menu').append('<li id="' + name + '"><a href="' + '#' + name + '">' + name + '</a></li>');
}

function selectMenuItem(name) {
    $('#menu li').removeClass('active');
    var element = $('#' + name);
    element.addClass('active');    
    
    var handler = handlers[name];
    if(handler != undefined) {
        handler();        
    }
}

function initFogFunctionExamples() 
{
    for(var i=0; i<myToplogyExamples.length; i++) {
        var example = myToplogyExamples[i];
        submitTopology(example.topology, example.designboard);
    }
}

function queryFogFunctions() 
{
    var queryReq = {}
    queryReq.entities = [{type:'FogFunction', isPattern: true}];
    client.queryContext(queryReq).then( function(fogFunctionList) {
        if (fogFunctionList.length == 0) {
			initFogFunctionExamples();
		}
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query fog functions');
    });          
}


function showFogFunctionEditor() 
{
    $('#info').html('to design a fog function');

    var html = '';
    
    html += '<div id="topologySpecification" class="form-horizontal"><fieldset>';            
    
    html += '<div class="control-group"><label class="control-label">name</label>';
    html += '<div class="controls"><input type="text" class="input-large" id="serviceName">';
    html += '</div></div>';
    
    html += '<div class="control-group"><label class="control-label">description</label>';
    html += '<div class="controls"><textarea class="form-control" rows="3" id="serviceDescription"></textarea>';
    html += '</div></div>';      
           
    html += '<div class="control-group"><label class="control-label">topology</label><div class="controls">';
    html += '<span>  </span><button id="cleanBoard" type="button" class="btn btn-default">Clean Board</button>';                            
    html += '<span>  </span><button id="saveBoard" type="button" class="btn btn-default">Save Board</button>';  
    html += '<span>  </span><button id="generateFunction" type="button" class="btn btn-primary">Submit</button>';                                      
    html += '</div></div>';   
       
    html += '</fieldset></div>';   
        
    html += '<div id="blocks" style="width:800px; height:400px"></div>';
       
    $('#content').html(html);    

    blocks = new Blocks();
 
    registerAllBlocks(blocks, operatorList);

    blocks.run('#blocks');
    
    blocks.types.addCompatibility('string', 'choice');
    
    if (CurrentScene != null ) {
        blocks.importData(CurrentScene);
    }
        
    blocks.ready(function() {                
        // associate functions to clickable buttons
        $('#generateFunction').click(function() {
            boardScene2Topology(blocks.export());
        });    
        $('#cleanBoard').click(function() {
            blocks.clear();
        });  
        $('#saveBoard').click(function() {
            CurrentScene = blocks.export();
        });                                              
    });    
           
}

function openTopologyEditor(topologyEntity)
{		
    if(topologyEntity.attributes.designboard){
        CurrentScene = topologyEntity.attributes.designboard.value;          
        showTopologyEditor(); 
        
        var topology = topologyEntity.attributes.template.value;
        
        $('#serviceName').val(topology.name);
        $('#serviceDescription').val(topology.description);
    }
}

function deleteTopology(topologyEntity)
{
    var entityid = {
        id : topologyEntity.entityId.id, 
        type: 'Topology',
        isPattern: false
    };	    
    
    client.deleteContext(entityid).then( function(data) {
        console.log(data);
		updateTopologyList();		
    }).catch( function(error) {
        console.log('failed to delete a service topology');
    });  	
}


function queryOperatorList()
{
    var queryReq = {}
    queryReq.entities = [{type:'Operator', isPattern: true}];           
    
    client.queryContext(queryReq).then( function(operators) {
        for(var i=0; i<operators.length; i++){
            var entity = operators[i];        
            var operator = entity.attributes.operator.value;                 
            operatorList.push(operator.name);              
    	} 
        
        // add it into the select list        
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query context');
    });    
}

function boardScene2Topology(scene)
{
    // step 1: construct the service topology object       
    var topologyName = $('#serviceName').val();
    var serviceDescription = $('#serviceDescription').val();

    var topology = {};    
    topology.name = topologyName;
    topology.description = serviceDescription;    
    topology.tasks = generateTaskList(scene);           

    var topologyCtxObj = {};    
    topologyCtxObj.entityId = {
        id : 'Topology.' + topology.name, 
        type: 'Topology',
        isPattern: false
    };    
    topologyCtxObj.attributes = {};   
    topologyCtxObj.attributes.status = {type: 'string', value: 'enabled'};
    topologyCtxObj.attributes.designboard = {type: 'object', value: scene};    
    topologyCtxObj.attributes.template = {type: 'object', value: topology};  


    // step 2: construct an intent object
    var intent = {};        
    intent.topology = topologyName;    
    intent.priority = {
        'exclusive': false,
        'level': 0
    };        
    intent.qos = "default";    
    intent.geoscope = {
        "scopeType": "local",
        "scopeValue": "local"
    };   
    
    var intentCtxObj = {};    
    intentCtxObj.entityId = { 
        id: 'ServiceIntent.' + uuid(),           
        type: 'ServiceIntent',
        isPattern: false
    };
    
    intentCtxObj.attributes = {};   
    intentCtxObj.attributes.status = {type: 'string', value: 'enabled'};
    intentCtxObj.attributes.intent = {type: 'object', value: intent};  
    
    // step 3: create this fog function            
    var functionCtxObj = {};    
    functionCtxObj.entityId = {
        id : 'FogFunction.' + topologyName, 
        type: 'FogFunction',
        isPattern: false
    };    
    functionCtxObj.attributes = {};   
    functionCtxObj.attributes.name = {type: 'string', value: topologyName};    
    functionCtxObj.attributes.topology = {type: 'object', value: topology};    
    functionCtxObj.attributes.intent = {type: 'object', value: intent};  
    functionCtxObj.attributes.status = {type: 'string', value: 'enabled'};        
    client.updateContext(functionCtxObj).then( function(data1) {
        console.log(data1);                 
    }).then( function(data2) {
        console.log(data2);                 
        client.updateContext(topologyCtxObj);        
    }).then( function(data3) {
        console.log(data3);                 
        client.updateContext(intentCtxObj);                
        
        showFogFunctions();
    }).catch( function(error) {
        console.log('failed to record the created fog function');
    });              
}


function generateTaskList(scene)
{    
    var tasklist = [];
    
    for(var i=0; i<scene.blocks.length; i++){
        var block = scene.blocks[i];
        if (block.type == 'Task') {            
            var task = {};
            
            task.name = block.values['name'];
            task.operator = block.values['operator'];

            task.input_streams = [];
            task.output_streams = [];
            
            // look for all input streams associated with this task
            task.input_streams = findInputStream(scene, block.id); 
                        
            // figure out the defined output stream types                        
            for(var j=0; j<block.values['outputs'].length; j++){
                var outputstream = {};
                outputstream.entity_type = block.values['outputs'][j];
                task.output_streams.push(outputstream);
            }
            
            tasklist.push(task);
        }
    }
    
    return tasklist;
}

function findInputStream(scene, blockid)
{
    var inputstreams = [];
    
    for(var i=0; i<scene.edges.length; i++) {
        var edge = scene.edges[i];
        if (edge.block2 == blockid) {
            var inputblockId = edge.block1;
            
            for(var j=0; j<scene.blocks.length; j++){
                var block = scene.blocks[j];
                if (block.id == inputblockId){
                    if (block.type == 'Shuffle') {                        
                        var inputstream = {};
                        
                        inputstream.selected_type = findInputType(scene,  block.id)          
                        
                        if (block.values['selectedattributes'].length == 1 && block.values['selectedattributes'][0].toUpperCase() == 'ALL') {
                            inputstream.selected_attributes = [];
                        } else {
                            inputstream.selected_attributes = block.values['selectedattributes'];                            
                        }
                        
                        inputstream.groupby = block.values['groupby'];                                                                        
                        inputstream.scoped = true;
                        
                        inputstreams.push(inputstream)
                    } else if (block.type == 'EntityStream') {
                        var inputstream = {};
                                                
                        inputstream.selected_type = block.values['selectedtype'];            
                        
                        if (block.values['selectedattributes'].length == 1 && block.values['selectedattributes'][0].toUpperCase() == 'ALL') {
                            inputstream.selected_attributes = [];
                        } else {
                            inputstream.selected_attributes = block.values['selectedattributes'];                            
                        }                                                            
                        
                        inputstream.groupby = block.values['groupby'];                                                
                        inputstream.scoped = block.values['scoped'];
                        
                        inputstreams.push(inputstream)
                    }
                }
            }
        }
    }        
    
    return inputstreams;
}

function findInputType(scene, blockId)
{
    var inputType = "unknown";

    for(var i=0; i<scene.edges.length; i++){
        var edge = scene.edges[i];
        
        if(edge.block2 == blockId) {
            var index = edge.connector1[2];     
            
            for(var j=0; j<scene.blocks.length; j++) {
                var block = scene.blocks[j];                
                if(block.id == edge.block1) {  
                    console.log(block);
                    inputType = block.values.outputs[index];                    
                }
            }               
        }
    }
    
    return inputType;
}

function showFogFunctions() 
{    
    $('#info').html('list of all registered fog functions');
    
    var html = '<div style="margin-bottom: 10px;"><button id="registerFunction" type="button" class="btn btn-primary">register</button></div>';
    html += '<div id="functionList"></div>';

	$('#content').html(html);   
      
    $( "#registerFunction" ).click(function() {
        showFogFunctionEditor();
    });    
                  
    // update the list of submitted fog functions
    updateFogFunctionList();    
}

function updateFogFunctionList() 
{
    var queryReq = {}
    queryReq.entities = [{type:'FogFunction', isPattern: true}];
    client.queryContext(queryReq).then( function(functionList) {
        console.log(functionList);
        displayFunctionList(functionList);
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query context');
    });       
}

function displayFunctionList(fogFunctions) 
{
    if(fogFunctions == null || fogFunctions.length == 0) {
        return        
    }
    
    var html = '<table class="table table-striped table-bordered table-condensed">';
   
    html += '<thead><tr>';
    html += '<th>ID</th>';
    html += '<th>Name</th>';        
    html += '<th>Topology</th>';            
    html += '<th>Intent</th>';                
    html += '</tr></thead>';    
       
    for(var i=0; i<fogFunctions.length; i++){
        var fogfunction = fogFunctions[i];
		
    	html += '<tr>'; 
		html += '<td>' + fogfunction.entityId.id;
		html += '<br><button id="editor-' + fogfunction.entityId.id + '" type="button" class="btn btn-default">editor</button>';
		html += '<br><button id="delete-' + fogfunction.entityId.id + '" type="button" class="btn btn-default">delete</button>';
		html += '</td>';        
               
        var topology = fogfunction.attributes.topology.value;
        
		html += '<td>' + topology.name + '</td>';                       
                  
		html += '<td>' + JSON.stringify(fogfunction.attributes.topology) + '</td>';                
		html += '<td>' + JSON.stringify(fogfunction.attributes.intent) + '</td>';                
        
		html += '</tr>';	
	}
       
    html += '</table>';  

	$('#functionList').html(html);  
    
    // associate a click handler to the editor button
    for(var i=0; i<fogFunctions.length; i++){
        var fogfunction = fogFunctions[i];
        
		// association handlers to the buttons
        var editorButton = document.getElementById('editor-' + fogfunction.entityId.id);
        editorButton.onclick = function(myFogFunction) {
            return function(){
                openFogFunctionEditor(myFogFunction);
            };
        }(fogfunction);
		
        var deleteButton = document.getElementById('delete-' + fogfunction.entityId.id);
        deleteButton.onclick = function(myFogFunction) {
            return function(){
                deleteFogFunction(myFogFunction);
            };
        }(fogfunction);		
	}        
}


function uuid() {
    var uuid = "", i, random;
    for (i = 0; i < 32; i++) {
        random = Math.random() * 16 | 0;
        if (i == 8 || i == 12 || i == 16 || i == 20) {
            uuid += "-"
        }
        uuid += (i == 12 ? 4 : (i == 16 ? (random & 3 | 8) : random)).toString(16);
    }
    
    return uuid;
}    
  


function showTaskInstances() 
{
    $('#info').html('list of running data processing tasks');

    var queryReq = {}
    queryReq.entities = [{type:'Task', isPattern: true}];    
    
    client.queryContext(queryReq).then( function(taskList) {
        console.log(taskList);
        displayTaskList(taskList);
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query context');
    });     
}

function displayTaskList(tasks) 
{
    if(tasks == null || tasks.length ==0){
        $('#content').html('');                   
        return
    }
    
    var html = '<table class="table table-striped table-bordered table-condensed">';
   
    html += '<thead><tr>';
    html += '<th>ID</th>';
    html += '<th>Type</th>';
    html += '<th>Attributes</th>';
    html += '<th>DomainMetadata</th>';    
    html += '</tr></thead>';    
       
    for(var i=0; i<tasks.length; i++){
        var task = tasks[i];
		
        html += '<tr>'; 
		html += '<td>' + task.entityId.id + '</td>';
		html += '<td>' + task.entityId.type + '</td>'; 
		html += '<td>' + JSON.stringify(task.attributes) + '</td>';        
		html += '<td>' + JSON.stringify(task.metadata) + '</td>';
		html += '</tr>';	
	}
       
    html += '</table>'; 

	$('#content').html(html);   
}


});



