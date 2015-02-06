var creationDate = 0;
var appNameSelected;
var BEGIN = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
	+ "\n"
	+ "<beans xmlns=\"http://www.springframework.org/schema/beans\""
	+ "\n"
	+ "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
	+ "\n"
	+ "xsi:schemaLocation=\"http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans-2.0.xsd\""
	+ "\n" + "default-lazy-init=\"false\">" + "\n\n";
var END = "\n\n</beans>";

$(document).ready(function() {
	$("#tabs li").removeClass();
	$("#configTab").addClass("active");
	listByApp();
	initAllBindings();
	$('#viewBeanDefInRB').on('hide',function(){
		$('#beansDivRB').modal('show');
	});
});

function initAllBindings(){
	$("#createBtn").bind('click', initCreateModal);
	$("#dataForm").bind('submit', submitDataForm);
	$("#appForm").bind('submit', submitAppForm);
	$("#validateBtn").bind('click', validateBeanDef);
	$("#fileBtn").click(function(){$("#chooseFileBtn").click();});
	$("#chooseFileBtn").bind('change', fileChoosed);
	$("#appNameSelector").bind('change', listByApp);
	$("#exportBtn").bind('click', exportBeanDef);
	$("#editDCBtn").bind('click', makeDCEditable);
	$("#addAppNameBtn").click(function(){initAppConfigInputs();});
	$("#setDCBtn").click(function(){updateSettingDialog(false);});
	$("#saveDCBtn").bind('click', updateDc);
	$("#scope").bind('change', selectScope);
	$("#parseBtn").bind('click', parseBeans);
	$("#pushBeansBtn").bind('click', batchOperate);
	$("#checkAll").click(function(){checkAll(this.checked, "beansTable");});
	$("#jetStreamTableCheck").click(function(){checkAll(this.checked, "jetStreamTable");});
	$("#diagramBtn").bind('click', openDiagram);
	$("#batchDeleteBtn").bind('click', batchDelete);
	$("#batchUpdateBtn").bind('click', initBatchUpdateDiv);
	$("#valiBeansBtn").bind('click', validateBeansDef);
	$("#expoBeansBtn").bind('click', exportBeansDef);
	$("#beansDivRB #cancelBtn").bind('click', closeBeanLog);
	$("#logBtn").bind('click',showBeanLog);
}

function validateBeansDef(){
	var index = 0;
	var beansDef = "";
	$("#beansTableBody tr").each(function(){
		var tds = $(this).find("td");
		var beanDefinition = $("#edit" + index).val();
		var bean = constructBeanJson(tds.eq(0).text(), 
				tds.eq(1).text(), beanDefinition, tds.eq(2).text(),
				tds.eq(3).text(), tds.eq(4).text(), "", "0");
		beansDef += (bean + ",");
		index++;
	});
	beansDef = beansDef.substring(0, beansDef.length-1);
	beansDef = "[" + beansDef + "]";
	validateBeans(beansDef, "validate", "valiBeansBtn", "beansMsg");
	return false;
}

function initBatchUpdateDiv(){
	var count = $("#jetStreamTableBody input:checkbox:checked").size();
	if(count < 1){
		alert("Please choose at least one bean.");
		return;
	}
	var beans = "";
	$("#beansTableBody").empty();
	$("#beansTable #checkCol").hide();
	$("#beanDefDetail").hide();
	$('#beansDefEdit').empty().show();
	$('#valiBeansBtn').show();
	$("#beansMsg").hide();
	$('#expoBeansBtn').show();
	$("#beansLabel").html("Batch Update");
	var index = 0;
	$("#jetStreamTableBody input:checkbox:checked").each(function(){
		onerow = [];
		var tds = $(this).parent().parent().find('td');
		onerow.push('<td>' + $(tds).eq(1).text() + "</td>");
		onerow.push('<td>' + $(tds).eq(2).text() + "</td>");
		onerow.push('<td>' + $(tds).eq(3).text() + "</td>");
		onerow.push('<td>' + $(tds).eq(4).text() + "</td>");
		onerow.push('<td>' + $(tds).eq(5).text() + "</td>");
		onerow.push('<td><a href="#" id="link' + index + '"><strong>Edit</strong></a></td>');		
		$('#beansTableBody').append('<tr>' + onerow.join('') + '</tr>');	
		$('#beansDefEdit').append('<textarea id="edit' + index + '" style="display:none;width:95%;height:250px;">' 
				+ htmlEncode($(tds).eq(10).find('div').eq(2).text()) + '</textarea>');
		index ++;
	 });
	$('#beansTableBody a[href="#"]').bind('click',showBeanDefEdit);
	$("#beansDiv").modal("show");
}

function showBeanDefEdit(){
	var linkId = $(this).attr("id").toString();
	var id = "edit" + linkId.substring(4, linkId.length);
	$('#beansDefEdit textarea').hide();
	$("#beansDefEdit #" + id).show();
}

function batchDelete(){
	var count = $("#jetStreamTableBody input:checkbox:checked").size();
	if(count < 1){
		alert("Please choose at least one bean.");
		return;
	}
	if (confirm("Delete the chosen " + count + " bean(s)?")) {
		var beans = "";
		 $("#jetStreamTableBody input:checkbox:checked").each(function(){
			 var tds = $(this).parent().parent().find('td');
			 var bean = constructBeanJson($(tds).eq(1).text(), 
					 $(tds).eq(2).text(), 
					 "", 
					 $(tds).eq(3).text(),
					 $(tds).eq(4).text(), 
					 $(tds).eq(5).text(),
					 "", "0");
			 beans += bean;
			 beans += ",";
		 });
		 beans = beans.substring(0, beans.length-1);
		 beans = "[" + beans + "]";
		$.ajax({
			url : "configuration/batchDelete",
			type : "POST",
			contentType : "application/json",
			dataType : "json",
			data: beans,
		}).success(function(data) {
			checkLogin(data);
			if (!data.success) {
				showErrorMsg("alertMessage", data.message);
			} else {
				showSuccessMsg("alertMessage" , "Delete successfully!");
				setTimeout("$(fadeAndReload('dataDiv'))", 1000);
			}
		}).error(function(jqXHR, textStatus, errorThrown) {
			checkIfToLogin(jqXHR);
			showErrorMsg("alertMessage", errorThrown);
		});
	}
}

function showErrorMsg(id, msg){
	$('#' + id).html(msg);
	$('#' + id).removeClass("alert-success");
	$('#' + id).addClass("alert-error");
	$('#' + id).css("display", "block");
}



function showSuccessMsg(id, msg){
	$('#' + id).html(msg);
	$('#' + id).addClass("alert-success");
	$('#' + id).removeClass("alert-error");
	$('#' + id).css("display", "block");
	fadeIn1Min(id);
}

function submitAppForm(){
	var name = $('#appForm #appName').val().trim();
	//var poolName = $('#appForm #pool').val().trim();
	//var machine = $('#appForm #machine').val().trim();
	//var port = $('#appForm #port').val().trim();
	appConfigOperation(name, "createApp",
			constructAppConfigJson(name));
					//machine, port));
	return false;
}

function submitDataForm(){
	$("#resultMessage").html("");
	var appName = $('#appName :selected').text().trim();
	var version = $('#version').val().trim();
	var beanDefinition = $('#beanDefinition').val()
			.trim();
	var beanName = $('#beanName').val().trim();
	var beanVersion = $('#beanVersion').val().trim();
	var scope = $('#scope :selected').text().trim();
	if(scope == "dc")
		scope += (":" + $('#dc :selected').text().trim());
	else if(scope == "local")
		scope += (":" + $('#local').val().trim());
	operate(appName, $("#saveBtn").attr('name'),
			constructBeanJson(appName, version,
					beanDefinition, beanName,
					beanVersion, scope, "", creationDate));
	return false;
}

function initCreateModal(){
	$("#dataDiv input").attr('disabled', false);
	$("#dataDiv select").attr('disabled', false);
	$("#dataDiv textarea").attr('disabled', false);
	$('div.uploadSpec').css("display", "block");
	$("#resultMessage").css("display", "none");
	$("#dataDiv :text").val("");
	$("#dataDiv textarea").val("");
	$("#appName option").attr("selected", false); 
	$("#scope").get(0).options[0].selected = true; 
	$("#dc").css("display", "none");
	$("#local").css("display", "none");
	$("#dataInputDiv").css("display", "block");
	$("#saveBtn").attr('name', 'create');
	$("#validateBtn").attr('name', 'validate');
	$("#exportBtn").show();
	$("#parseBtn").hide();
	$("#saveBtn").show();
	$("#setDCBtn").hide();
	$("#beanNameDiv").hide();
	$("#beanName").val("beanName");
	var app = $('#appNameSelector :selected').text().trim();
	if(app != "All"){
		setSelected("appName" , app);
	}
}

function openDiagram(){
	var url='configuration/diagram?app=' + appNameSelected;
	var fwindow=window.open(url, 'diagram','width=1200,height=530,alwaysRaised=yes,scrollbars=yes,resizable=yes,top=90,left=90');
	fwindow.focus();
}

function batchUpload(){
	var beans = "";
	 $("#beansTableBody input:checkbox:checked").each(function(){
		 var tds = $(this).parent().parent().find('td');
		 var appName = $(tds).eq(0).text();
		 selectedAppChanged(appName);
		 var scope = $(tds).eq(4).find("#scope :selected").eq(0).text().trim();
		 if(scope == "dc")
			 scope += (":" + $(tds).eq(4).find("#dc :selected").eq(0).text().trim());
		 else if(scope == "local")
			 scope += (":" + $(tds).eq(4).find("#local").eq(0).val().trim());
		 var bean = constructBeanJson($(tds).eq(0).text(), 
				 $(tds).eq(1).text(), 
				 $(tds).eq(5).find('div').eq(0).text(), 
				 $(tds).eq(2).text(),
				 $(tds).eq(3).text(), 
				 scope,
				 "", creationDate);
		 beans += bean;
		 beans += ",";
	 });
	 beans = beans.substring(0, beans.length-1);
	 beans = "[" + beans + "]";
	$.ajax({
		url : "configuration/batchCreate",
		type : "POST",
		contentType : "application/json",
		dataType : "json",
		data: beans,
	}).success(function(data) {
		checkLogin(data);
		$("#beansMsg").css("display", "block");
		if (!data.success) {
			showErrorMsg("beansMsg", data.message);
		} else {
			showSuccessMsg("beansMsg" , "Create beans successfully!");
			setTimeout("$(fadeAndReload('beansDiv'))", 1000);
		}
	}).error(function(jqXHR, textStatus, errorThrown) {
		checkIfToLogin(jqXHR);
		showErrorMsg("beansMsg", errorThrown);
	});
}

function batchOperate(){
	if($("#beansLabel").html() == "Batch Upload")
		batchUpload();
	else
		batchUpdate();
}

function batchUpdate(){
	var beans = "";
	 $("#beansTableBody tr").each(function(){
		 var tds = $(this).find('td');
		 var appName = $(tds).eq(0).text();
		 var linkId = $(tds).eq(5).find('a').eq(0).attr('id').toString();
		 var editId = "edit" + linkId.substring(4, linkId.length);
		 var bean = constructBeanJson($(tds).eq(0).text(), 
				 $(tds).eq(1).text(), 
				 $("#" + editId).val(), 
				 $(tds).eq(2).text(),
				 $(tds).eq(3).text(), 
				 $(tds).eq(4).text(), 
				 "", "0");
		 beans += bean;
		 beans += ",";
	 });
	 beans = beans.substring(0, beans.length-1);
	 beans = "[" + beans + "]";
	 $("#pushBeansBtn").text("Pushing...");
	 $("#beansFooterDiv button").attr("disabled", true);
	 var updateType="update";
	 if($("#beansLabel").text()=="Log Update"){
			updateType="rollback";
	 }
	$.ajax({
		url : "configuration/batchUpdate?type="+updateType,
		type : "POST",
		contentType : "application/json",
		dataType : "json",
		data: beans,
	}).success(function(data) {
		$("#pushBeansBtn").text("Save & Apply");
		$("#beansFooterDiv button").attr("disabled", false);
		checkLogin(data);
		if (!data.success) {
			showErrorMsg("beansMsg", data.message);
		} else {
			showSuccessMsg("beansMsg" , "Update beans successfully!");
			setTimeout("$(fadeAndReload('beansDiv'))", 1000);
		}
	}).error(function(jqXHR, textStatus, errorThrown) {
		checkIfToLogin(jqXHR);
		$("#pushBeansBtn").text("Save & Apply");
		$("#beansFooterDiv button").attr("disabled", false);
		showErrorMsg("beansMsg", errorThrown);
	});
}

function selectedAppChanged(newVal){
	appNameSelected = newVal;
	if(appNameSelected == "All"){
		$("#diagramBtn").attr("disabled", true);
	}else{
		$("#diagramBtn").attr("disabled", false);
	}
}

function parseBeans(){
	$("#beansMsg").css("display", "none");
	var dataPara = {};
	dataPara["beanDefinition"] = $("#beanDefinition").val();
	var appName = $('#appName :selected').text().trim();
	var version = $('#version').val().trim();
	if(version == ""){
		showErrorMsg("resultMessage", "version must neither be null nor empty");
		return;
	}
	$.ajax({
		url : "configuration/parseBeans",
		type : 'POST',
		data : dataPara,
		error : function(data){
			alert(data);
		},
		success: function(data){
			checkLogin(data);
			if(data.success){
				$('#dataDiv').modal('hide');
				initBeanModal(data.payload, appName, version);
				$('#beansDiv').modal('show');
			}else{
				showErrorMsg("resultMessage", data.message);
			}
		}
	});
}

function initBeanModal(beans, appName, version){
	$('#beansTableBody').empty();
	$("#beansTable input:checkbox").each(function(){
		this.checked = true;
	});
	$('#beanDefDetail').hide();
	$('#beansDefEdit').hide();
	$('#valiBeansBtn').hide();
	$('#expoBeansBtn').hide();
	$("#beansLabel").html("Batch Upload");
	$("#beansTable #checkCol").show();
	var scope = '<select id="scope"><option selected value="global">global</option>'
			+ '<option value="dc">dc</option>'
			+ '<option value="local">local</option>'
			+ '</select><select style="display:none;" id="dc">';
	$('#dc option').each(function(){
		scope += ('<option>' + $(this).val() + '</option>');
	});
	scope += '</select><input id="local" type="text" style="display:none" placeholder="machine name"/>';
	scope += "<a href='#settingDCDiv' data-toggle='modal' id='setDCBtn' style='display:none;white-space:nowrap;'>can't find dc?</a>";
	for (var index = 0; index < beans.length; index++) {
		onerow = [];
		onerow.push('<td>' + appName + "</td>");
		onerow.push('<td>' + version + "</td>");
		onerow.push('<td>' + beans[index]["beanName"] + "</td>");
		onerow.push('<td>1</td>');
		onerow.push('<td>' + scope + '</td>');
		onerow.push('<td><a href="#"><strong>View</strong></a><div style="display:none;">'
				+ htmlEncode(beans[index]["beanDefinition"]) + '</div></td>');		
		onerow.push('<td style="text-align:center;"><input type="checkbox" checked="true"/></td>');
		$('#beansTableBody').append('<tr>' + onerow.join('') + '</tr>');	
	}
	$('#beansTableBody select').css("width", "100px").css("margin-right", "5px");
	$('#beansTableBody input').css("width", "100px").css("margin-right", "5px");
	$('#beansTableBody #scope').bind('change',selectScope);
	$('#beansTableBody a[href="#settingDCDiv"]').click(function(){updateSettingDialog(false);});
	$('#beansTableBody a[href="#"]').bind('click',showBeanDefInBean);
}

function showBeanDefInBean(){
	var beanDef = $(this).parent().find("div").eq(0).text();
	$("#beanDefDetail").text(beanDef);
	$("#beanDefDetail").show();
}

function fileChoosed(){
	var selected_file = $('#chooseFileBtn').get(0).files[0];
	var reader = new FileReader();  
	reader.readAsText(selected_file);
	reader.onload = function(e){
		initUploadFile();
		$("#dataDiv").modal('show');
		$("#beanDefinition").val(this.result);
		$("#chooseFileBtn").val("");
	}
}

function selectScope(){
	var parent = $(this).parent();
	var id = $(this).attr("id");
	var scope = $(parent).find("#scope :selected").eq(0).text().trim();
	if(scope == "dc"){
		$(parent).find("#dc").eq(0).show();
		$(parent).find("#setDCBtn").eq(0).show();
		$(parent).find("#local").eq(0).hide();
	}else if(scope == "local"){
		$(parent).find("#dc").eq(0).hide();
		$(parent).find("#setDCBtn").eq(0).hide();
		$(parent).find("#local").eq(0).show();
	}else{
		$(parent).find("#dc").eq(0).hide();
		$(parent).find("#setDCBtn").eq(0).hide();
		$(parent).find("#local").eq(0).hide();
	}
}

function updateDc(){
	$("#settingDCDiv button").attr('disabled', true);
	$("#saveDCBtn").text("Saving...");
	var dataPara = {};
	dataPara['list'] = $("#dcEdit").val();
	$.ajax({
		url : "settings/updateDC",
		type : "POST",
		data: dataPara
	}).success(function(data) {
		checkLogin(data);
		if(data.success){
			$("#setDCMsg").css("display", "block");
			updateDC(data.payload);
			setTimeout('$("#settingDCDiv").modal("hide")', 1000);
		}else if(data.success == false){
			alert(data.message);
		}
	}).error({	
	});
}

function updateDC(dcs){
	var dcArray = dcs.split(",");
	$("#dc").empty();
	$("#setDCSelector").empty();
	$("#beansTableBody tr #dc").empty();
	for(var dc in dcArray){
		$("#dc").append('<option>' + dcArray[dc] + '</option>');
		$("#setDCSelector").append('<option>' + dcArray[dc] + '</option>');
		$("#beansTableBody tr #dc").append('<option>' + dcArray[dc] + '</option>');
	}
	$("#setDCSelector").attr("size", dcArray.length);
}

function updateSettingDialog(isEdit){
	if(isEdit){
		$("#editDCBtn").hide();
		$("#saveDCBtn").show();
		$("#dcEdit").show();
		$("#setDCSelector").hide();
	}else{
		$("#editDCBtn").show();
		$("#saveDCBtn").hide();
		$("#setDCSelector").show();
		$("#dcEdit").hide();
	}
	$("#settingDCDiv button").attr('disabled', false);
	$("#saveDCBtn").text("Save");
	$('#setDCMsg').css('display', 'none');
}

function makeDCEditable(){
	var value = "";
	var rows = 0;
	var settingName = "dc";
	$("#setDCSelector option").each(function(){
		value += ($(this).val() + "\r\n");
		rows ++;
	});
	$("#dcEdit").val(value);
	$("#dcEdit").attr('rows', rows);
	updateSettingDialog(true);
}

function listByApp(){
	var appName = $('#appNameSelector :selected').text().trim();
	selectedAppChanged(appName);
	if(appName == "All"){
		$.ajax({
			url : "configuration/listAll",
			type : "GET",
			contentType : "application/json",
			dataType : "json",
		}).success(function(data) {
			$("#loading").css("display", "none");
			showTableData(data);
		}).error({});
	}
	else{
		$.ajax({
			url : "configuration/listByApp",
			type : "POST",
			contentType : "application/json",
			dataType : "json",
			data: constructBeanJson(appName, "", "", "", "", "","", creationDate),
		}).success(function(data) {
			$("#loading").css("display", "none");
			showTableData(data);
		}).error({});
	}
}

function initUploadFile(){
	$('#appName').attr('disabled', false);
	$('#version').attr('disabled', false);
	$("#dataDiv input:button").attr('disabled', false);
	$("#dataDiv button").attr('disabled', false);
	$('div.uploadSpec').css("display", "none");
	$("#resultMessage").css("display", "none");
	$("#saveBtn").css("display", "none");
	$("#dataDiv :text").val("");
	$("#dataDiv textarea").val("");
	$("#beanName").val("beanName");
	$("#dataInputDiv").css("display", "block");
	$("#saveBtn").attr('name', 'upload');
	$("#validateBtn").attr('name', 'validateFile');
	$("#exportBtn").show();
	$("#parseBtn").show();
	$('#version').val("");
	$("#appName option").attr("selected", false); 
	var app = $('#appNameSelector :selected').text().trim();
	if(app != "All"){
		setSelected("appName" , app);  
	}
}

function validateBeanDef(){
	var appName = $('#appName :selected').text().trim();
	var version = $('#version').val().trim();
	var beanDefinition = $('#beanDefinition').val().trim();
	var beanName = $('#beanName').val().trim();
	var beanVersion = $('#beanVersion').val().trim();
	var scope = $('#scope :selected').text().trim();
	if(scope == "dc")
		scope += (":" + $('#dc :selected').text().trim());
	else if(scope == "local")
		scope += (":" + $('#local').val().trim());
	var beanDefJson = constructBeanJson(appName, version, beanDefinition, beanName,
			beanVersion, scope, "", creationDate);
	var operation = $("#validateBtn").attr('name');
	if(operation == "validate")
		beanDefJson = "[" + beanDefJson + "]";
	validateBeans(beanDefJson, operation, "validateBtn", "resultMessage");
	return false;
}

function validateBeans(bean, operation, valiBtn, msgId){
	$("#" + valiBtn).val("validating...");
	$.ajax({
		url:"configuration/" + operation,
		type:'POST',
		contentType : "application/json",
		dataType : "json",
		data:bean,
		success: function(data){
			checkLogin(data);
			$("#" + valiBtn).val("Validate");
			if(data.success){
				showSuccessMsg(msgId, "Validate successfully!");
			}else{
				showErrorMsg(msgId, data.message);
			}
		},
		error: function(jqXHR, textStatus, errorThrown){
			checkIfToLogin(jqXHR);
			$("#valiBeansBtn").text("Validate");
			showErrorMsg(msgId, errorThrown);
		}
	});
}

function fadeIn1Min(id){
	setTimeout("$('#" + id + "').css('display', 'none')", 1000);
}

function fadeAndReload(modalName) {
	$('#' + modalName).modal('hide');
	window.location.href="configuration?appNameSelected=" + appNameSelected;
}

function closeBeanLog(){
    window.location.href="configuration?appNameSelected=" + appNameSelected;
}

function operate(app, func, jsonStr) {
	$.ajax({
		url : "configuration/" + func,
		type : "POST",
		contentType : "application/json",
		dataType : "json",
		data : jsonStr,
	}).success(function(data) {
		checkLogin(data);
		if (!data.success) {
			showErrorMsg("resultMessage", data.message);
		} else {
			showSuccessMsg("resultMessage", func + " successfully!");
			selectedAppChanged(app);
			setTimeout("$(fadeAndReload('dataDiv'))", 1000);
		}
	}).error(function(jqXHR, textStatus, errorThrown) {
		checkIfToLogin(jqXHR);
		showErrorMsg("resultMessage", errorThrown);
	});
}

function appConfigOperation(app, func, jsonStr) {
	$.ajax({
		url : "settings/" + func,
		type : "POST",
		contentType : "application/json",
		dataType : "json",
		data : jsonStr,
	}).success(function(data) {
		checkLogin(data);
		if (!data.success) {
			showErrorMsg("appFormMsg", data.message);
		} else {
			showSuccessMsg("appFormMsg", func + " successfully!");
			selectedAppChanged(app);
			setTimeout("$(fadeAndReload('appDiv'))", 1000);
		}
	}).error(function(jqXHR, textStatus, errorThrown) {
		checkIfToLogin(jqXHR);
		showErrorMsg("appFormMsg", errorThrown);
	});
}

function showTableData(rows) {
	$('#jetStreamTableBody').empty();
	if (rows == null || rows.length < 1) {
		return;
	}
	for ( var index in rows) {
		onerow = [];
		onerow.push('<td style="text-align:center;"><input type="checkbox"/></td>');
		onerow.push('<td>' + rows[index].appName + "</td>");
		onerow.push('<td>' + rows[index].version + "</td>");
		onerow.push('<td>' + rows[index].beanName + "</td>");
		onerow.push('<td>' + rows[index].beanVersion + "</td>");
		onerow.push('<td>' + rows[index].scope + "</td>");
		onerow.push('<td>' + rows[index].createdBy + "</td>");
		onerow.push('<td>' + rows[index].creationDateAsString
				+ "<div style='display: none;'>" + rows[index].creationDate + "</div></td>");
		onerow.push('<td>' + rows[index].modifiedBy + "</td>");
		onerow.push('<td>' + rows[index].modifiedDateAsString
				+ "<div style='display: none;'>" + rows[index].modifiedDate + "</div></td>");
		var beanDef = htmlEncode(rows[index].beanDefinition);
		onerow.push('<td><div>'
				+ "</div><div><a href='#viewBeanDef' data-toggle='modal'><strong>View<strong></a></div>"
				+ "<div style='display:none;'>" + beanDef + "</div></td>");
		$('#jetStreamTableBody').append('<tr>' + onerow.join('') + '</tr>');
	}
	$("#jetStreamTableBody a[href='#viewBeanDef']").bind('click', showBeanDefinition);
	checkAll($("#jetStreamTableCheck").checked, "jetStreamTable");
}

function exportBeanDef(){
	var content = $("#beanDefinition").val().trim(); 
	if($("div.uploadSpec").css("display") == "block")
		content = BEGIN + content + END;
	export2File(content);
}

function export2File(content){
	if(!/msie/i.test(navigator.userAgent)){
    	$("#exportLink").attr("href", "data:application/xml;base64,"
            + Base64.encode(content));
    	$("#exportLink").get(0).click();
	}
}

function exportBeansDef(){
	var content = "";
	$("#beansDefEdit textarea").each(function(){
		content += "\n\n\t";
		content += $(this).val().trim();
	});
	content = BEGIN + content + END;
	export2File(content);
}

function showBeanDefinition(){
	var td = this.parentNode.parentNode;
	var beanDef =  $(td).find('div').eq(2).text();
	$('#viewBeanDefBody').text(beanDef);
}

function showBeanDefinitionInRB(link){
	 var td = link.parentNode.parentNode;
	 var beanDef =  $(td).find('div').eq(2).text();
	 $('#viewBeanDefBodyInRB').text(beanDef);
	 $('#beanVersionRB').text("Bean Version : " + $(td).parent().children().eq(3).text());
	 $("#beansDivRB").modal("hide");
	 $('#viewBeanDefInRB').modal('show');
}

function showBeanLog() {
	reloadBeanLog();
}

function reloadBeanLog(){
	$.ajax({
		url : "configuration/listBeanLog",
		type : "GET",
		contentType : "application/json",
		dataType : "json",
	}).success(function(data) {
		$("#loading").css("display", "none");
		populateData(data);
		$("#beansDivRB").modal("show");
	}).error(function(jqXHR, textStatus, errorThrown) {
		checkIfToLogin(jqXHR);
		showErrorMsg("beansMsgRB", errorThrown);
	});
}

function populateData(data) {
	$("#beansTableBodyRB").empty();
	$("#beanDefDetail").hide();
	$('#beansDefEdit').empty().show();
	$("#beansMsg").hide();
	for(var index in data) {
	    var row = [];    
	    row.push("<td style='width:20%;'>");
	    
	    var pushBtn = "<button class='btn btn-info btn-mini pull-right' onclick='pushRollBack(this)'>Push</button>";
	    row.push('<span>'+data[index].appName+'</span>'+pushBtn+'</td>');
	    
	    
	    row.push('<td>'+data[index].version+'</td>');
	    row.push('<td>'+data[index].beanName+'</td>');
	    row.push('<td>'+data[index].beanVersion+'</td>');
	    row.push('<td>'+data[index].scope+'</td>');
	    row.push('<td>'+data[index].createdBy+'</td>');
	    row.push('<td>'+data[index].creationDateAsString+'</td>');
	    row.push('<td>'+data[index].modifiedBy+'</td>');
	    row.push('<td>'+data[index].modifiedDateAsString+'</td>');
	    row.push('<td>'+data[index].operatedBy+'</td>');
	    row.push('<td>'+data[index].operatedDateAsString+'</td>');
	    
	    var beanDef = htmlEncode(data[index].beanDefinition);
	    var status = data[index].status;
	    var statusStr;
	    if(status==0){
	    	statusStr = '<span class="label label-warning">Updated</span>';
	    }else if(status==1){
	    	statusStr = '<span class="label label-important">Deleted</span>';
	    }
	    row.push('<td>'+statusStr+'</td>');
	    row.push('<td><div>'
				+ "</div><div><a href='#' onclick='javascript:showBeanDefinitionInRB(this);'><strong>View<strong></a></div>");
	    row.push("<div style='display:none;'>" + beanDef + "</div></td>");
	    $("#beansTableBodyRB").append('<tr>'+row.join('')+'</tr>');
	}
	
	 $("#beansTableRB").dataTable({
		 retrieve: true,
		 "order": [[ 10, "desc" ]],
		 "aoColumnDefs":[{"sWidth":"30%","aTargets":[0]}]
	} );
	
	// $("#beansTableRB").dataTable();
}

function pushRollBack(btn) {
	var tds = $(btn).parent().parent().children();
	var beanVersion = $(tds).eq(3).text();
	var appName=$(tds).eq(0).find('span').text();
	if(!confirm("Do you want to push "+ appName +" (bean version " + beanVersion+ ")?")){
		return;
	}
	
	var appName=$(tds).eq(0).find('span').text();
	var version=$(tds).eq(1).text();
	var beanName = $(tds).eq(2).text();
	var beanVersion = $(tds).eq(3).text();
	var scope =  $(tds).eq(4).text();	
	var beanDef = $(tds).eq(12).find('div').eq(2).text();
	
	$("#beansTableBody").empty();
	$("#beansTable #checkCol").hide();
	$("#beanDefDetail").hide();
	$('#beansDefEdit').empty().show();
	$('#valiBeansBtn').show();
	$("#beansMsg").hide();
	$('#expoBeansBtn').show();
	$('#pushBeansBtn').show();
	$("#beansLabel").html("Log Update");
	
	var index = 0;
	onerow = [];
	var tds = $(this).parent().parent().find('td');
	onerow.push('<td>' + appName + "</td>");
	onerow.push('<td>' + version + "</td>");
	onerow.push('<td>' + beanName + "</td>");
	onerow.push('<td>' + beanVersion + "</td>");
	onerow.push('<td>' + scope + "</td>");
	onerow.push('<td><a href="#" id="link' + index + '"><strong>Edit</strong></a></td>');		
	$('#beansTableBody').append('<tr>' + onerow.join('') + '</tr>');	
	$('#beansDefEdit').append('<textarea id="edit' + index + '" style="display:none;width:95%;height:250px;">' 
			+ htmlEncode(beanDef) + '</textarea>');
	index ++;
	
	$('#beansDiv #beansTableBody a[href="#"]').bind('click',showBeanDefEdit);
	$("#beansDivRB").modal("hide");
	$("#beansDiv").modal("show");
}

function initScope(scope){
	var scopes = scope.split(":");
	$("#scope option").attr("selected", false); 
	setSelected("scope" , scopes[0]);   
	if(scopes[0] == "global"){
		$("#dc").css("display", "none");
		$("#setDCBtn").css("display", "none");
		$("#local").css("display", "none");
	}else if(scopes[0] == "dc"){
		$("#dc").css("display", "inline-block");
		$("#setDCBtn").css("display", "inline-block");
		$("#local").css("display", "none");
		$("#dc option").attr("selected", false); 
		setSelected("dc" , scopes[1]);   
	}else{
		$("#dc").css("display", "none");
		$("#setDCBtn").css("display", "none");
		$("#local").css("display", "inline-block");
		$("#local").val(scopes[1]);
	}
}