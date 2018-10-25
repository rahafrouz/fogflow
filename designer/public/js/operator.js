$(function(){

// initialization  
var handlers = {};  

//connect to the broker
var client = new NGSI10Client(config.brokerURL);

addMenuItem('Operator', showOperator);  
addMenuItem('FunctionCode', showFunctionCode);  
addMenuItem('DockerImage', showDockerImage);    

initDockerImageList();

showOperator();


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
    if(handler != null) {
        handler();        
    }
}



function showDesignBoard()
{
    var html = '';
    
    html += '<div class="input-prepend">';         
    html += '<button id="cleanBoard" type="button" class="btn btn-default">Clean Board</button>';                            
    html += '<button id="saveBoard" type="button" class="btn btn-default">Save Board</button>';                                
    html += '<button id="generateFunction" type="button" class="btn btn-primary">Create a Fog Function</button>';        
    html += '<button id="displayFogFunctionObject" type="button" class="btn btn-default">Display as JSON</button>';         
    html += '</div>'; 
        
    html += '<div id="blocks" style="width:1000px; height:400px"></div>';
    
    html += '<div style="margin-top: 10px;"><h4 class="text-left">Function code</h4>';
    html += '<select id="codeType"><option value="javascript">javascript</option><option value="python"">python</option><option value="docker"">dockerimage</option></select>';    
    html += '<div id="codeBoard"></div>';            
    html += '</div>'    
    
    $('#content').html(html);  
    
    var boardHTML = '<textarea id="codeText" class="form-control" style="min-width: 800px; min-height: 200px"></textarea>';
    $('#codeBoard').html(boardHTML);
    $('#codeText').val(template.javascript);
   
	
    var blocks = new Blocks();
 
    // prepare the configuration
    var config = {};

    // prepare the design board
    registerAllBlocks(blocks);
  
    blocks.run('#blocks');
    
    if (CurrentScene != null ) {
		console.log(CurrentScene);
        blocks.importData(CurrentScene);
    }  		
	
    blocks.ready(function() {                
        $('#generateFunction').click(function() {
            generateFogfunction(blocks.export());
        });    
        $('#cleanBoard').click(function() {
            blocks.clear();
        });                      
        $('#saveBoard').click(function() {
            CurrentScene = blocks.export();
        });                              
        $('#displayFogFunctionObject').click(function() {
            var board = blocks.export();
            var fogfunction = boardScene2fogfunction(board);    
            var ffObj = {
                fogfunction: fogfunction,
                designboard: board
            };
            alert(JSON.stringify(ffObj));
        });                                      
    }); 	  	
}


function showEditor() 
{
    $('#info').html('editor to design a fog function');
    
	showDesignBoard();
    
    $('#codeType').change(function() {
        var fType = $(this).val();
        switch(fType) {
            case 'javascript':
                var boardHTML = '<textarea id="codeText" class="form-control" style="min-width: 800px; min-height: 200px"></textarea>';
                $('#codeBoard').html(boardHTML);
                $('#codeText').val(template.javascript);
                break;
            case 'python':
                var boardHTML = '<textarea id="codeText" class="form-control" style="min-width: 800px; min-height: 200px"></textarea>';
                $('#codeBoard').html(boardHTML);
                $('#codeText').val(template.python);
                break;
            case 'docker':
                var boardHTML = '<select id="codeImage"></select>';
                $('#codeBoard').html(boardHTML);                
                for(var i=0; i<operatorList.length; i++){
                    var operatorName = operatorList[i];
                    $('#codeImage').append($("<option></option>").attr("value", operatorName).text(operatorName));                                                    
                }                
                break;
        }        
    });    
    
    //initialize the content in the code textarea
    $('#codeText').val(template.javascript);              
}


function showOperator() 
{
    $('#info').html('list of all registered operators');

    var queryReq = {}
    queryReq.entities = [{type:'DockerImage', isPattern: true}];           
    
    client.queryContext(queryReq).then( function(imageList) {
        console.log(imageList);

        for(var i=0; i<imageList.length; i++){
            var dockerImage = imageList[i];            
            var operatorName = dockerImage.attributes.operator.value;
            
            var exist = false;
            for(var j=0; j<operatorList.length; j++){
                if(operatorList[j] == operatorName){
                    exist = true;
                    break;
                }
            }
            
            if(exist == false){
                operatorList.push(operatorName);                
            }            
        }
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query the operator list');
    });     
}

function generateFogfunction(scene)
{
    // construct the fog function object based on the design board
    var fogfunction = boardScene2fogfunction(scene);    
   
    // submit this fog function
    submitFunction(fogfunction, scene);
}


function queryFunctionList() 
{
    var queryReq = {}
    queryReq.entities = [{type:'FogFunction', isPattern: true}];
    client.queryContext(queryReq).then( function(fogFunctionList) {
        if (fogFunctionList.length == 0) {
			initFogFunctionExamples();
		}
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query task');
    });          
}


function boardScene2fogfunction(scene)
{
    console.log(scene);  
    var fogfunction = {};    
    
    // check the function type and the provided function code
    var fType = $('#codeType option:selected').val();    
    fogfunction.type = fType;
    
    switch(fType) {
        case 'javascript':
            var fCode = $('#codeText').val();
            fogfunction.code = fCode;    
            fogfunction.dockerImage = 'nodejs';           
            break;
        case 'python':
            var fCode = $('#codeText').val();
            fogfunction.code = fCode;    
            fogfunction.dockerImage = 'pythonbase';           
            break;
        case 'docker':
            var dockerImage = $('#codeImage option:selected').val();            
            fogfunction.code = '';    
            fogfunction.dockerImage = dockerImage;           
            break;        
    }     
    
    // check the defined inputs and outputs of this function
    for(var i=0; i<scene.blocks.length; i++){
        var block = scene.blocks[i];
        
        console.log(block.name);
        
        if(block.type == "FogFunction") {
            fogfunction.name = block.values['name'];
            fogfunction.user = block.values['user'];
            
            // construct its input streams
            fogfunction.inputTriggers = findInputTriggers(scene, block.id);
            
            // construct its output streams
            fogfunction.outputAnnotators = findOutputAnnotators(scene, block.id);   
            
            break;         
        }
    }        
    
    return fogfunction;    
}

function findInputTriggers(scene, blockId)
{
    var selectors = [];

    for(var i=0; i<scene.edges.length; i++){
        var edge = scene.edges[i];
        
        if(edge.block2 == blockId) {
            for(var j=0; j<scene.blocks.length; j++) {
                var block = scene.blocks[j];
                
                if(block.id == edge.block1) {
                    var selector = {};
                    selector.name = "selector" + block.id
                    selector.selectedAttributeList = block.values.selectedattributes;
                    selector.groupedAttributeList = block.values.groupby;
                    selector.conditionList = findConditions(scene, block.id);
                    
                    selectors.push(selector);
                }
            }               
        }
    }
    
    return selectors;
}


function findConditions(scene, blockId)
{
    var conditions = [];
    
    for(var i=0; i<scene.edges.length; i++){
        var edge = scene.edges[i];    
        
        if(edge.block2 == blockId) {        
            for(var j=0; j<scene.blocks.length; j++) {
                var block = scene.blocks[j];
                
                if(block.id == edge.block1) {        
                    var condition = {};                    
                    condition.type = block.values.type;
                    condition.value = block.values.value;                                    
                    conditions.push(condition);
                }
            }
        }
    }
            
    return conditions
}

function findOutputAnnotators(scene, blockId)
{
    var annotators = [];
    
    for(var i=0; i<scene.edges.length; i++){
        var edge = scene.edges[i];    
        
        if(edge.block1 == blockId) {                    
            for(var j=0; j<scene.blocks.length; j++) {
                var block = scene.blocks[j];
                
                if(block.id == edge.block2) {        
                    var annotator = {};    
                    
                    annotator.entityType = block.values.entitytype;
                    annotator.groupInherited = block.values.herited;                
                    
                    annotators.push(annotator);
                }
            }
        }
    }            
    
    return annotators;    
}


function submitFunction(fogfunction, designboard)
{
	console.log("==============================")
    console.log(JSON.stringify(fogfunction));  
	console.log(JSON.stringify(designboard));
	console.log("============end===============")

        
    var functionCtxObj = {};
    
    functionCtxObj.entityId = {
        id : 'FogFunction.' + fogfunction.name, 
        type: 'FogFunction',
        isPattern: false
    };
    
    functionCtxObj.attributes = {};   
    functionCtxObj.attributes.status = {type: 'string', value: 'enabled'};
    functionCtxObj.attributes.designboard = {type: 'object', value: designboard};    	
    functionCtxObj.attributes.fogfunction = {type: 'object', value: fogfunction};    
    
    client.updateContext(functionCtxObj).then( function(data) {
        console.log(data);  
              
        // update the list of submitted topologies
        updateFogFunctionList();                       
    }).catch( function(error) {
        console.log('failed to submit the fog function');
    });           
}

function showFunctionCode() 
{
    var queryReq = {}
    queryReq.entities = [{type:'FunctionCode', isPattern: true}];
    client.queryContext(queryReq).then( function(functionList) {
        console.log(functionList);
        displayFunctionList(functionList);
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query task');
    });          
}

function updateFogFunctionList() 
{
    var queryReq = {}
    queryReq.entities = [{type:'FunctionCode', isPattern: true}];
    client.queryContext(queryReq).then( function(functionList) {
        console.log(functionList);
        displayFunctionList(functionList);
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query context');
    });       
}

function displayFunctionList(functions) 
{
    $('#info').html('list of all submitted fog functions');

    if(functions.length == 0) {
        $('#content').html('');
        return;
    }          

    var html = '<table class="table table-striped table-bordered table-condensed">';
   
    html += '<thead><tr>';
    html += '<th>ID</th>';
    html += '<th>FogFunction</th>';
    html += '<th>Status</th>';    
    html += '</tr></thead>';    

    for(var i=0; i<functions.length; i++){
        var functionitem = functions[i];
        
        html += '<tr>';		
		html += '<td>' + functionitem.entityId.id + '<br><button id="editor-' + functionitem.entityId.id + '" type="button" class="btn btn-default">editor</button>';        
		html += '<br><button id="delete-' + functionitem.entityId.id + '" type="button" class="btn btn-default">delete</button></td>';        
		html += '<td>' + JSON.stringify(functionitem.attributes['fogfunction'].value) + '</td>'; 
        
        var status = functionitem.attributes['status'].value;        
                     
		html += '</tr>';			
	}
       
    html += '</table>';            
	
	$('#content').html(html);        	
}


function switchFogFunctionStatus(fogFunc)
{
    var functionCtxObj = {};    
    
    // switch the status
    functionCtxObj.entityId = fogFunc.entityId
    
    functionCtxObj.attributes = {};   
    
    if (fogFunc.attributes.status.value == "enabled") {
        functionCtxObj.attributes.status = {type: 'string', value: 'disabled'};        
    } else {
        functionCtxObj.attributes.status = {type: 'string', value: 'enabled'};        
    }
    
    client.updateContext(functionCtxObj).then( function(data) {
        console.log(data);                
        // update the list of submitted topologies
        updateFogFunctionList();                       
    }).catch( function(error) {
        console.log('failed to submit the topology');
    });      
}

function deleteFunction(fogFunc)
{
    var entityid = {
        id : fogFunc.entityId.id, 
        type: 'FogFunction',
        isPattern: false
    };	    
    
    client.deleteContext(entityid).then( function(data) {
        console.log(data);
		updateFogFunctionList();		
    }).catch( function(error) {
        console.log('failed to delete a fog function');
    });  	
}

function showFunction() 
{
    $('#info').html('list of all triggerred function tasks');
            
    var queryReq = {}
    queryReq.entities = [{type:'Function', isPattern: true}];

    client.queryContext(queryReq).then( function(functionList) {
        console.log(functionList);
        displayFunctionList(functionList);
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query task');
    });        
}


function displayFunctionList(functions) 
{
    $('#info').html('list of all function tasks that have been triggerred');

    if(functions.length == 0) {
        $('#content').html('');
        return;
    }          

    var html = '<table class="table table-striped table-bordered table-condensed">';
   
    html += '<thead><tr>';
    html += '<th>ID</th>';
    html += '<th>Type</th>';
    html += '<th>Attributes</th>';	
    html += '<th>DomainMetadata</th>';		
    html += '</tr></thead>';    

    for(var i=0; i<functions.length; i++){
        var func = functions[i];
        html += '<tr>';
		html += '<td>' + func.entityId.id + '</td>';
		html += '<td>' + func.entityId.type + '</td>'; 
		html += '<td>' + JSON.stringify(func.attributes) + '</td>';        
		html += '<td>' + JSON.stringify(func.metadata) + '</td>';
		html += '</tr>';			
	}
       
    html += '</table>';            
	
	$('#content').html(html);      
}


function openEditor(fogfunctionEntity)
{
    if(fogfunctionEntity.attributes.designboard){
        CurrentScene = fogfunctionEntity.attributes.designboard.value;   	
    }
		
	//selectMenuItem('Editor');
	//window.location.hash = '#Editor';			
	showEditor();
       
    var fogfunction = fogfunctionEntity.attributes.fogfunction.value;
			    		
	// check the function type and the provided function code
	$('#codeType').val(fogfunction.type);
	
    switch(fogfunction.type) {
        case 'javascript':		
            var boardHTML = '<textarea id="codeText" class="form-control" style="min-width: 800px; min-height: 200px"></textarea>';
            $('#codeBoard').html(boardHTML);		
       		$('#codeText').val(fogfunction.code);          
       		break;
   		case 'python':			
            var boardHTML = '<textarea id="codeText" class="form-control" style="min-width: 800px; min-height: 200px"></textarea>';
            $('#codeBoard').html(boardHTML);		
       		$('#codeText').val(fogfunction.code);          
       		break;
   		case 'docker':				
            var boardHTML = '<select id="codeImage"></select>';
            $('#codeBoard').html(boardHTML); 
            for(var i=0; i<operatorList.length; i++){
                var operatorName = operatorList[i];
                $('#codeImage').append($("<option></option>").attr("value", operatorName).text(operatorName));                                                    
            } 			               
       		$('#codeImage').val(fogfunction.dockerImage);                     
       		break;        
	} 
}



function showDockerImage() 
{
    $('#info').html('list of docker images in the docker registry');

    var html = '<div style="margin-bottom: 10px;"><button id="registerDockerImage" type="button" class="btn btn-primary">register</button></div>';
    html += '<div id="dockerImageList"></div>';

	$('#content').html(html);   
      
    updateDockerImageList();       
    
    $( "#registerDockerImage" ).click(function() {
        dockerImageRegistration();
    });                
}


function initDockerImageList()
{
    var imageList = [{
        name: "fogflow/nodejs",
        tag: "latest",
        hwType: "X86",
        osType: "Linux",
        operatorName: "nodejs",
        prefetched: true
    },{
        name: "fogflow/python",
        tag: "latest",
        hwType: "X86",
        osType: "Linux",
        operatorName: "python",
        prefetched: false
    },{
        name: "fogflow/counter",
        tag: "latest",
        hwType: "X86",
        osType: "Linux",
        operatorName: "counter",
        prefetched: false
    },{
        name: "fogflow/anomaly",
        tag: "latest",
        hwType: "X86",
        osType: "Linux",
        operatorName: "anomaly",
        prefetched: false
    },{
        name: "fogflow/connectedcar",
        tag: "latest",
        hwType: "X86",
        osType: "Linux",
        operatorName: "connectedcar",
        prefetched: false
    },{
        name: "fogflow/recommender",
        tag: "latest",
        hwType: "X86",
        osType: "Linux",
        operatorName: "recommender",
        prefetched: false
    },{
        name: "fogflow/privatesite",
        tag: "latest",
        hwType: "X86",
        osType: "Linux",
        operatorName: "privatesite",
        prefetched: false
    },{
        name: "fogflow/publicsite",
        tag: "latest",
        hwType: "X86",
        osType: "Linux",
        operatorName: "publicsite",
        prefetched: false
    }
    ];

    for(var i=0; i<imageList.length; i++) {
        addDockerImage(imageList[i]);
    }
}

function addDockerImage(image) 
{    
    //register a new docker image
    var newImageObject = {};

    newImageObject.entityId = {
        id : image.name + ':' + image.tag, 
        type: 'DockerImage',
        isPattern: false
    };

    newImageObject.attributes = {};   
    newImageObject.attributes.image = {type: 'string', value: image.name};        
    newImageObject.attributes.tag = {type: 'string', value: image.tag};    
    newImageObject.attributes.hwType = {type: 'string', value: image.hwType};      
    newImageObject.attributes.osType = {type: 'string', value: image.osType};          
    newImageObject.attributes.operator = {type: 'string', value: image.operatorName};      
    newImageObject.attributes.prefetched = {type: 'boolean', value: image.prefetched};                      
    
    newImageObject.metadata = {};    
    newImageObject.metadata.operator = {
        type: 'string',
        value: image.operatorName
    };               

    client.updateContext(newImageObject).then( function(data) {
        console.log(data);
    }).catch( function(error) {
        console.log('failed to register the new device object');
    });      
    
}

function dockerImageRegistration()
{
    $('#info').html('to register a new docker image');
    
    var html = '<div id="dockerRegistration" class="form-horizontal"><fieldset>';                 
    
    html += '<div class="control-group"><label class="control-label" for="input01">Image(*)</label>';
    html += '<div class="controls"><input type="text" class="input-xlarge" id="dockerImageName">';
    html += '</div></div>';
    
    html += '<div class="control-group"><label class="control-label" for="input01">Tag(*)</label>';
    html += '<div class="controls"><input type="text" class="input-xlarge" id="imageTag" placeholder="latest">';
    html += '</div></div>';    
    
    html += '<div class="control-group"><label class="control-label" for="input01">HardwareType(*)</label>';
    html += '<div class="controls"><select id="hwType"><option>X86</option><option>ARM</option></select></div>'
    html += '</div>';    
    
    html += '<div class="control-group"><label class="control-label" for="input01">OSType(*)</label>';
    html += '<div class="controls"><select id="osType"><option>Linux</option><option>Windows</option></select></div>'
    html += '</div>';    

    html += '<div class="control-group"><label class="control-label" for="input01">Operator(*)</label>';
    html += '<div class="controls"><input type="text" class="input-xlarge" id="OperatorName">';
    html += '</div></div>';    

    html += '<div class="control-group"><label class="control-label" for="optionsCheckbox">Prefetched</label>';
    html += '<div class="controls"> <label class="checkbox"><input type="checkbox" id="Prefetched" value="option1">';
    html += 'docker image must be fetched by the platform in advance';
    html += '</label></div>';
    html += '</div>';        

    
    html += '<div class="control-group"><label class="control-label" for="input01"></label>';
    html += '<div class="controls"><button id="submitRegistration" type="button" class="btn btn-primary">Register</button>';
    html += '</div></div>';   
       
    html += '</fieldset></div>';

	$('#content').html(html);          
        
    // associate functions to clickable buttons
    $('#submitRegistration').click(registerDockerImage);  
}


function registerDockerImage() 
{    
    console.log('register a new docker image'); 

    // take the inputs    
    var image = $('#dockerImageName').val();
    console.log(image);
    
    var tag = $('#imageTag').val();
    if (tag == '') {
        tag = 'latest';
    }
    
    console.log(tag);    
    
    var hwType = $('#hwType option:selected').val();
    console.log(hwType);
    
    var osType = $('#osType option:selected').val();
    console.log(osType);    
    
    var operatorName = $('#OperatorName').val();
    console.log(operatorName);        
    
    var prefetched = document.getElementById('Prefetched').checked;
    console.log(prefetched);        
    
               
    if( image == '' || tag == '' || hwType == '' || osType == '' || operatorName == '' ) {
        alert('please provide the required inputs');
        return;
    }    

    //register a new docker image
    var newImageObject = {};

    newImageObject.entityId = {
        id : image + ':' + tag, 
        type: 'DockerImage',
        isPattern: false
    };

    newImageObject.attributes = {};   
    newImageObject.attributes.image = {type: 'string', value: image};        
    newImageObject.attributes.tag = {type: 'string', value: tag};    
    newImageObject.attributes.hwType = {type: 'string', value: hwType};      
    newImageObject.attributes.osType = {type: 'string', value: osType};          
    newImageObject.attributes.operator = {type: 'string', value: operatorName};  
    
    if (prefetched == true) {
        newImageObject.attributes.prefetched = {type: 'boolean', value: true};                      
    } else {
        newImageObject.attributes.prefetched = {type: 'boolean', value: false};                      
    }
            
    newImageObject.metadata = {};    
    newImageObject.metadata.operator = {
        type: 'string',
        value: operatorName
    };               

    client.updateContext(newImageObject).then( function(data) {
        console.log(data);
        
        // show the updated image list
        showDockerImage();
    }).catch( function(error) {
        console.log('failed to register the new device object');
    });      
    
}


function updateDockerImageList()
{
    var queryReq = {}
    queryReq.entities = [{type:'DockerImage', isPattern: true}];           
    
    client.queryContext(queryReq).then( function(imageList) {
        console.log(imageList);
        displayDockerImageList(imageList);
    }).catch(function(error) {
        console.log(error);
        console.log('failed to query context');
    });    
}

function displayDockerImageList(images) 
{
    if(images == null || images.length == 0){
        $('#dockerImageList').html('');           
        return        
    }
    
    var html = '<table class="table table-striped table-bordered table-condensed">';
   
    html += '<thead><tr>';
    html += '<th>Operator</th>';    
    html += '<th>Image</th>';
    html += '<th>Tag</th>';
    html += '<th>Hardware Type</th>';
    html += '<th>OS Type</th>';    
    html += '<th>Prefetched</th>';        
    html += '</tr></thead>';    
       
    for(var i=0; i<images.length; i++){
        var dockerImage = images[i];
		
        html += '<tr>'; 
		html += '<td>' + dockerImage.attributes.operator.value + '</td>';                
		html += '<td>' + dockerImage.attributes.image.value + '</td>';                
		html += '<td>' + dockerImage.attributes.tag.value + '</td>';        
		html += '<td>' + dockerImage.attributes.hwType.value + '</td>';                
		html += '<td>' + dockerImage.attributes.osType.value + '</td>';  
        
        if (dockerImage.attributes.prefetched.value == true) {
		    html += '<td><font color="red"><b>' + dockerImage.attributes.prefetched.value + '</b></font></td>';                                            
        } else {
		    html += '<td>' + dockerImage.attributes.prefetched.value + '</td>';                                            
        }
                              
		html += '</tr>';	                        
	}
       
    html += '</table>';  
    
	$('#dockerImageList').html(html);      
}

});


