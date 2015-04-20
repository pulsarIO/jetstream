function emptyInputs(divId){
	$("#" + divId + " input").val("");
}

function checkAll(checked, parentNodeId){
	$("#" + parentNodeId + " input:checkbox").each(function(){
		this.checked = checked;
	});
}

function constructAppConfigJson(name) { //, machine, port
	var jsonStr = "\"name\":\"" + name + "\"";
	//jsonStr += ", \"machine\":\"" + machine + "\"";
	//jsonStr += ", \"port\":\"" + port + "\"";
	return '{' + jsonStr + '}';
}

function constructBeanJson(appName, version, beanDefinition, beanName,
		beanVersion, scope, createdBy, creationDate) {
	var jsonStr = "\"appName\":\"" + appName + "\"";
	if (version != "") {
		jsonStr += ", \"version\":\"" + version + "\"";
	}
	jsonStr += ", \"beanDefinition\":\"" + encodeURIComponent(beanDefinition) + "\"";
	jsonStr += ", \"beanName\":\"" + beanName + "\"";
	if (beanVersion != "") {
		jsonStr += ", \"beanVersion\":\"" + beanVersion + "\"";
	}
	if (scope != "") {
		jsonStr += ", \"scope\":\"" + scope + "\"";
	}
	jsonStr += ", \"createdBy\":\"" + createdBy + "\"";
	jsonStr += ", \"creationDate\":\"" + creationDate + "\"";
	return '{' + jsonStr + '}';
}

function validateNumber(e, isNumberOnly) {
	var k = window.event ? e.keyCode : e.which;
	if (((k >= 48) && (k <= 57)) || k == 8) {
	} else if(k == 46 && !isNumberOnly){
	}else {
		if (window.event) {
			window.event.returnValue = false;
		} else {
			e.preventDefault(); //for firefox 
		}
	}
}

function htmlEncode(str) {
	var div = document.createElement("div");
	var text = document.createTextNode(str);
	div.appendChild(text);
	var result = div.innerHTML.toString();
	return result;
}

function setSelected(selectId, text){
	for(var i = 0; i < $("#" + selectId + " option").length;i++){           
		if($("#" + selectId).get(0).options[i].text == text)  {
			$("#" + selectId).get(0).options[i].selected = true;  
			break;  
		}  
	}  
}

function checkLogin(data){
	if(data.success == null)
		window.location.href="login";
}


function checkIfToLogin(jqXHR) {
	var text = jqXHR.responseText;
	if (text != null && text.indexOf("j_spring_security_login") > -1) {
		window.location = "/login.jsp";
	}
}
