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
<<<<<<< HEAD
    name: "Test",
    topology: {"entityId":{"id":"Topology.Test","type":"Topology","isPattern":false},"attributes":{"designboard":{"type":"object","value":{"edges":[{"id":1,"block1":2,"connector1":["stream","output"],"block2":1,"connector2":["streams","input"]}],"blocks":[{"id":1,"x":123,"y":-99,"type":"Task","module":null,"values":{"name":"Main","operator":"dummy","outputs":["Out"]}},{"id":2,"x":-194,"y":-97,"type":"EntityStream","module":null,"values":{"selectedtype":"Hello","selectedattributes":["all"],"groupby":"EntityID","scoped":false}}]}},"template":{"type":"object","value":{"name":"Test","description":"just for a simple test","tasks":[{"name":"Main","operator":"dummy","input_streams":[{"selected_type":"Hello","selected_attributes":[],"groupby":"EntityID","scoped":false}],"output_streams":[{"entity_type":"Out"}]}]}}}},
    intent:  {"entityId":{"id":"ServiceIntent.13d3d575-80cc-4ff5-934b-56d32251b94b","type":"ServiceIntent","isPattern":false},"attributes":{"status":{"type":"string","value":"enabled"},"intent":{"type":"object","value":{"topology":"Test","priority":{"exclusive":false,"level":0},"qos":"Max Throughput","geoscope":{"scopeType":"local","scopeValue":"local"}}}}}
=======
    "fogfunction":{"type":"docker","code":"","dockerImage":"privatesite","name":"PrivateSite","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["all"],"groupedAttributeList":["id"],"conditionList":[{"type":"EntityType","value":"PrivateSite"}]}],"outputAnnotators":[]},
    "designboard":{"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":24.549998298828086,"y":-148.75000475292967,"type":"FogFunction","module":null,"values":{"name":"PrivateSite","user":"fogflow"}},{"id":2,"x":-197.4500017011719,"y":-146.75000475292967,"type":"InputTrigger","module":null,"values":{"selectedattributes":["all"],"groupby":["id"]}},{"id":3,"x":-428.4500017011719,"y":-145.08333299999998,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"PrivateSite"}}]}
},
{
    "fogfunction":{"type":"docker","code":"","dockerImage":"publicsite","name":"PublicSite","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["all"],"groupedAttributeList":["id"],"conditionList":[{"type":"EntityType","value":"PublicSite"}]}],"outputAnnotators":[]},
    "designboard":{"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":24.549998298828086,"y":-148.75000475292967,"type":"FogFunction","module":null,"values":{"name":"PublicSite","user":"fogflow"}},{"id":2,"x":-197.4500017011719,"y":-146.75000475292967,"type":"InputTrigger","module":null,"values":{"selectedattributes":["all"],"groupby":["id"]}},{"id":3,"x":-428.4500017011719,"y":-145.08333299999998,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"PublicSite"}}]}
},
{
    "fogfunction":{"type":"docker","code":"","dockerImage":"recommender","name":"Recommender","user":"fogflow","inputTriggers":[{"name":"selector3","selectedAttributeList":["ParkingRequest"],"groupedAttributeList":["id"],"conditionList":[{"type":"EntityType","value":"ConnectedCar"}]}],"outputAnnotators":[]},
    "designboard":{"edges":[{"id":2,"block1":3,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":3,"block1":4,"connector1":["condition","output"],"block2":3,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":104.54999829882809,"y":-135.75000475292967,"type":"FogFunction","module":null,"values":{"name":"Recommender","user":"fogflow"}},{"id":4,"x":-445.4166459882813,"y":-141.75000475292967,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"ConnectedCar"}},{"id":3,"x":-179.4166459882813,"y":-147.75000475292967,"type":"InputTrigger","module":null,"values":{"selectedattributes":["ParkingRequest"],"groupby":["id"]}}]}
},
{
    "fogfunction":{"type":"docker","code":"","dockerImage":"connectedcar","name":"ConnectedCar","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["all"],"groupedAttributeList":["id"],"conditionList":[{"type":"EntityType","value":"ConnectedCar"}]}],"outputAnnotators":[]},
    "designboard":{"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":70.4081801170098,"y":-124.33545929838425,"type":"FogFunction","module":null,"values":{"name":"ConnectedCar","user":"fogflow"}},{"id":2,"x":-170.0545471557174,"y":-124.36545929838422,"type":"InputTrigger","module":null,"values":{"selectedattributes":["all"],"groupby":["id"]}},{"id":3,"x":-407.87272897389914,"y":-123.54727748020238,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"ConnectedCar"}}]}
},
{
    fogfunction: {"type":"docker","code":"","dockerImage":"pushbutton","name":"Pushbutton","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["all"],"groupedAttributeList":["id"],"conditionList":[{"type":"EntityType","value":"Pushbutton"}]}],"outputAnnotators":[]},
    designboard: {"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":96,"y":-133,"type":"FogFunction","module":null,"values":{"name":"Pushbutton","user":"fogflow"}},{"id":2,"x":-141,"y":-134,"type":"InputTrigger","module":null,"values":{"selectedattributes":["all"],"groupby":["id"]}},{"id":3,"x":-373,"y":-136,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"Pushbutton"}}]}
},
{
    fogfunction: {"type":"docker","code":"","dockerImage":"acoustic","name":"Acoustic","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["all"],"groupedAttributeList":["id"],"conditionList":[{"type":"EntityType","value":"Microphone"}]}],"outputAnnotators":[]},
    designboard: {"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":96,"y":-133,"type":"FogFunction","module":null,"values":{"name":"Acoustic","user":"fogflow"}},{"id":2,"x":-141,"y":-134,"type":"InputTrigger","module":null,"values":{"selectedattributes":["all"],"groupby":["id"]}},{"id":3,"x":-373,"y":-136,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"Microphone"}}]}
},
{
    fogfunction: {"type":"docker","code":"","dockerImage":"speaker","name":"Speaker","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["all"],"groupedAttributeList":["id"],"conditionList":[{"type":"EntityType","value":"Speaker"}]}],"outputAnnotators":[]},
    designboard: {"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":96,"y":-133,"type":"FogFunction","module":null,"values":{"name":"Speaker","user":"fogflow"}},{"id":2,"x":-141,"y":-134,"type":"InputTrigger","module":null,"values":{"selectedattributes":["all"],"groupby":["id"]}},{"id":3,"x":-373,"y":-136,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"Speaker"}}]}
},{
	fogfunction: {"type":"docker","code":"","dockerImage":"converter","name":"Converter1","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["location"],"groupedAttributeList":["all"],"conditionList":[{"type":"EntityType","value":"RainSensor"}]}],"outputAnnotators":[{"entityType":"RainObservation","groupInherited":false}]},
	designboard: {"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]},{"id":3,"block1":1,"connector1":["annotators","output"],"block2":4,"connector2":["annotator","input"]}],"blocks":[{"id":1,"x":13.549998298828086,"y":-144.75000475292967,"type":"FogFunction","module":null,"values":{"name":"Converter1","user":"fogflow"}},{"id":2,"x":-192.4500017011719,"y":-143.75000475292967,"type":"InputTrigger","module":null,"values":{"selectedattributes":["location"],"groupby":["all"]}},{"id":3,"x":-415.4500017011719,"y":-146.08333299999998,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"RainSensor"}},{"id":4,"x":236,"y":-144,"type":"OutputAnnotator","module":null,"values":{"entitytype":"RainObservation","herited":false}}]}
},{
	fogfunction: {"type":"docker","code":"","dockerImage":"geohash","name":"Converter2","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["location"],"groupedAttributeList":["all"],"conditionList":[{"type":"EntityType","value":"SmartAwning"}]}],"outputAnnotators":[]},
	designboard: {"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":13.549998298828086,"y":-144.75000475292967,"type":"FogFunction","module":null,"values":{"name":"Converter2","user":"fogflow"}},{"id":2,"x":-192.4500017011719,"y":-143.75000475292967,"type":"InputTrigger","module":null,"values":{"selectedattributes":["location"],"groupby":["all"]}},{"id":3,"x":-413.4500017011719,"y":-145.08333299999998,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"SmartAwning"}}]}
},{
	fogfunction: {"type":"docker","code":"","dockerImage":"converter","name":"Converter3","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["location"],"groupedAttributeList":["all"],"conditionList":[{"type":"EntityType","value":"ConnectedCar"}]}],"outputAnnotators":[{"entityType":"RainObservation","groupInherited":false}]},
	designboard: {"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]},{"id":3,"block1":1,"connector1":["annotators","output"],"block2":4,"connector2":["annotator","input"]}],"blocks":[{"id":1,"x":13.549998298828086,"y":-144.75000475292967,"type":"FogFunction","module":null,"values":{"name":"Converter3","user":"fogflow"}},{"id":2,"x":-192.4500017011719,"y":-143.75000475292967,"type":"InputTrigger","module":null,"values":{"selectedattributes":["location"],"groupby":["all"]}},{"id":3,"x":-415.4500017011719,"y":-146.08333299999998,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"ConnectedCar"}},{"id":4,"x":264,"y":-144,"type":"OutputAnnotator","module":null,"values":{"entitytype":"RainObservation","herited":false}}]}
},{
	fogfunction: {"type":"docker","code":"","dockerImage":"predictor","name":"Prediction","user":"fogflow","inputTriggers":[{"name":"selector3","selectedAttributeList":["geohash"],"groupedAttributeList":["geohash"],"conditionList":[{"type":"EntityType","value":"RainObservation"}]}],"outputAnnotators":[{"entityType":"Prediction","groupInherited":false}]},
	designboard: {"edges":[{"id":1,"block1":1,"connector1":["annotators","output"],"block2":2,"connector2":["annotator","input"]},{"id":2,"block1":3,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":3,"block1":4,"connector1":["condition","output"],"block2":3,"connector2":["conditions","input"]}],"blocks":[{"id":1,"x":-21.450001701171914,"y":-117.75000475292967,"type":"FogFunction","module":null,"values":{"name":"Prediction","user":"fogflow"}},{"id":2,"x":233.5833540117187,"y":-111.75000475292967,"type":"OutputAnnotator","module":null,"values":{"entitytype":"Prediction","herited":false}},{"id":3,"x":-240.4166459882813,"y":-117.75000475292967,"type":"InputTrigger","module":null,"values":{"selectedattributes":["geohash"],"groupby":["geohash"]}},{"id":4,"x":-468.4166459882813,"y":-114.75000475292967,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"RainObservation"}}]}
},{
	fogfunction: {"type":"docker","code":"","dockerImage":"controller","name":"Controller","user":"fogflow","inputTriggers":[{"name":"selector2","selectedAttributeList":["geohash"],"groupedAttributeList":["id"],"conditionList":[{"type":"EntityType","value":"SmartAwning"}]}],"outputAnnotators":[{"entityType":"ControlAction","groupInherited":false}]},
	designboard: {"edges":[{"id":1,"block1":2,"connector1":["selector","output"],"block2":1,"connector2":["selectors","input"]},{"id":2,"block1":3,"connector1":["condition","output"],"block2":2,"connector2":["conditions","input"]},{"id":3,"block1":1,"connector1":["annotators","output"],"block2":4,"connector2":["annotator","input"]}],"blocks":[{"id":1,"x":30.408180117009806,"y":-127.33545929838425,"type":"FogFunction","module":null,"values":{"name":"Controller","user":"fogflow"}},{"id":2,"x":-172.0545471557174,"y":-128.36545929838422,"type":"InputTrigger","module":null,"values":{"selectedattributes":["geohash"],"groupby":["id"]}},{"id":3,"x":-373.87272897389914,"y":-126.54727748020238,"type":"SelectCondition","module":null,"values":{"type":"EntityType","value":"SmartAwning"}},{"id":4,"x":250.5499982988281,"y":-128.33333299999998,"type":"OutputAnnotator","module":null,"values":{"entitytype":"ControlAction","herited":false}}]}
>>>>>>> master
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
    for(var i=0; i<myFogFunctionExamples.length; i++) {
        var fogfunction = myFogFunctionExamples[i];      
        
        var functionCtxObj = {};    
        functionCtxObj.entityId = {
            id : 'FogFunction.' + fogfunction.name, 
            type: 'FogFunction',
            isPattern: false
        };    
        functionCtxObj.attributes = {};   
        functionCtxObj.attributes.name = {type: 'string', value: fogfunction.name};    
        functionCtxObj.attributes.topology = {type: 'object', value: fogfunction.topology};    
        functionCtxObj.attributes.intent = {type: 'object', value: fogfunction.intent};  
        functionCtxObj.attributes.status = {type: 'string', value: 'enabled'};         
          
        submitFogFunction(functionCtxObj);
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

function openFogFunctionEditor(fogfunction)
{
    var topologyEntity = fogfunction.attributes.topology.value;
    
    if(topologyEntity &&  topologyEntity.attributes.designboard){
        CurrentScene = topologyEntity.attributes.designboard.value;          
        showFogFunctionEditor(); 
        
        var topology = topologyEntity.attributes.template.value;        
        $('#serviceName').val(topology.name);
        $('#serviceDescription').val(topology.description);
    }
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
    functionCtxObj.attributes.topology = {type: 'object', value: topologyCtxObj};    
    functionCtxObj.attributes.intent = {type: 'object', value: intentCtxObj};  
    functionCtxObj.attributes.status = {type: 'string', value: 'enabled'};    
    
    submitFogFunction(functionCtxObj).then(showFogFunctions);
}

function submitFogFunction(functionCtxObj)
{
    console.log("=============submit a fog function=============");
    console.log(JSON.stringify(functionCtxObj));
    
    var  topologyCtxObj = functionCtxObj.attributes.topology.value;
    var  intentCtxObj = functionCtxObj.attributes.intent.value;       
    
    return client.updateContext(functionCtxObj).then( function(data1) {
        console.log(data1);                 
    }).then( function(data2) {
        console.log(data2);                 
        client.updateContext(data2);        
    }(topologyCtxObj)).then( function(data3) {
        console.log(data3);                 
        client.updateContext(data3);                        
    }(intentCtxObj)).catch( function(error) {
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
                       
		html += '<td>' + JSON.stringify(fogfunction.attributes.name) + '</td>';                                  
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


function deleteFogFunction(fogfunction)
{
    // delete the related intent object   
    var intent = fogfunction.attributes.intent.value; 
    var intentEntity = {
        id : intent.entityId.id, 
        type: 'ServiceIntent',
        isPattern: false
    };	        
    client.deleteContext(intentEntity).then( function(data) {
        console.log(data);
    }).catch( function(error) {
        console.log('failed to delete the intent entity');
    });  	
    
    // delete the related service topology
    var topology = fogfunction.attributes.topology.value; 
    var topologyEntity = {
        id : topology.entityId.id, 
        type: 'Topology',
        isPattern: false
    };	        
    client.deleteContext(topologyEntity).then( function(data) {
        console.log(data);
    }).catch( function(error) {
        console.log('failed to delete the intent entity');
    });  	  	
    
    
    // delete this fog function
    var functionEntity = {
        id : fogfunction.entityId.id, 
        type: 'FogFunction',
        isPattern: false
    };	    
    
    client.deleteContext(functionEntity).then( function(data) {
        console.log(data);
		showFogFunctions();		
    }).catch( function(error) {
        console.log('failed to delete a service topology');
    });  	
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



