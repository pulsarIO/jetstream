<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Diagram</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="../resources/js/jquery-1.10.2.js"></script>
<script type="text/javascript" src="../resources/js/jquery-ui-1.9.2-min.js"></script>
<script type="text/javascript" src="../resources/js/jquery.jsPlumb-1.5.4.js"></script>

<script>
jsPlumb.ready(function() {
	if($("#error").text() == ""){
		var data = eval('('+$("#data").text()+')'); 
		var top = -130;
		var left = 200;
		for(var element in data["nodes"]){
			var id = data["nodes"][element].toString();
			var name = id;
			if(name.length > 20){
				name = name.substring(0, 20) + "...";
			}
			if(element % 3 == 0){
				top += 180;
				left = 200;
			}
			$("#Sample").append("<div class='element' id='" + id +  "' style='top:" + top + "px;left:" + left + "px;'><strong>" + name + "</strong></div>");
			left += 300;
		}

		jsPlumb.importDefaults({
	        DragOptions : { cursor: 'pointer', zIndex:2000 },
	        PaintStyle : { strokeStyle:'#666' },
	        EndpointStyle : { width:20, height:16, strokeStyle:'#666' },
	        Endpoint : "Rectangle",
	        Anchors : ["TopCenter"],
	        ConnectionOverlays : [[ "Arrow", { location:1 } ]]
	    });

	    var jetDropOptions = {
	        hoverClass:"dropHover",
	        activeClass:"dragActive"
	    };
	    
	    var color1 = "#5c96bc";
	    var jetEndpoint = {
	        endpoint:["Dot", { radius:2 }],
	        paintStyle:{ fillStyle:color1 },
	        isSource:true,
	        scope:"green dot",
	        connectorStyle:{ strokeStyle:color1, lineWidth:2, outlineColor:"transparent", outlineWidth:4 },
	        connector: ["Bezier", { curviness:63 } ],
	        maxConnections:1,
	        isTarget:true,
	        dropOptions : jetDropOptions
	    };

	    var sourceAnchors = [[0.2, 0, 0, -1, 0, 0, "foo"], [1, 0.2, 1, 0, 0, 0, "bar"], [0.8, 1, 0, 1, 0, 0, "baz"], [0, 0.8, -1, 0, 0, 0, "qux"] ],
		targetAnchors = [[0.6, 0, 0, -1], [1, 0.6, 1, 0], [0.4, 1, 0, 1], [0, 0.4, -1, 0] ];
	    
    	for (var e in data["nodes"]) {
    		var sourceId = data["nodes"][e];
    		if (data["graph"][sourceId]) {
    			for (var j = 0; j < data["graph"][sourceId].length; j++) {		
    				var targetId = data["graph"][sourceId][j];
    				jsPlumb.connect({
    					source:jsPlumb.addEndpoint(sourceId, jetEndpoint, {anchor:sourceAnchors}),
    					target:jsPlumb.addEndpoint(targetId, jetEndpoint, {anchor:targetAnchors})
    				});						
    			}
    		}	
    	} 
    	
    	jsPlumb.draggable($("#Sample .element"));
	}	
 });
</script>

<style>
	body {
        padding-top: 60px;
        padding-bottom: 40px;
	}
	#Sample .element{
		width:180px; 
		height:80px;
		line-height:80px;
		padding:8px;	
		background-color: white;
		border: 1px solid #346789;
		box-shadow: 2px 2px 19px #e0e0e0;
			-o-box-shadow: 2px 2px 19px #e0e0e0;
			-webkit-box-shadow: 2px 2px 19px #e0e0e0;
			-moz-box-shadow: 2px 2px 19px #e0e0e0;
			-moz-border-radius: 8px;
			border-radius: 8px;
		color: black;
		-webkit-transition: -webkit-box-shadow 0.15s ease-in;
		-moz-transition: -moz-box-shadow 0.15s ease-in;
		-o-transition: -o-box-shadow 0.15s ease-in;
		transition: box-shadow 0.15s ease-in;
		position: absolute;
	}
	#Sample .element:hover{
		box-shadow: 2px 2px 19px #444;
	   -o-box-shadow: 2px 2px 19px #444;
	   -webkit-box-shadow: 2px 2px 19px #444;
	   -moz-box-shadow: 2px 2px 19px #444;
	    opacity:0.6;
		filter:alpha(opacity=60);
	}
	._jsPlumb_connector { z-index:4; }
	._jsPlumb_endpoint { z-index:5; }
	#error {
		color: red;
		font-size: 30px;
		font-weight: bold;
		text-align: center;
	}
</style>
</head>
<body>
<%
	String error = (String)request.getAttribute("error");
	String data = (String)request.getAttribute("data");
	if(error == null)
		out.print("<div style='display:none;' id='data'>"+ data +"</div>");
	else
		out.print("<div id='error'>"+ error +"</div>");
%>
<div id="Sample" class="Sample"></div>
</body>
