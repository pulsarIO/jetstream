<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>JetStream Configuration Settings</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="../resources/css/bootstrap.css" rel="stylesheet">
<script src="../resources/js/jquery-1.10.2.js"></script>
<script src="../resources/js/bootstrap.js"></script>
<script src="../resources/js/JetStreamCommon.js"></script>
<style>
	table {
		width: 40%;
		text-align: center;
		border-width: initial;
		font-size: 16px;
	}
	label {
		font-weight: bold;
	}
	body {
        padding-top: 60px;
        padding-bottom: 40px;
	}
	textarea{
		font-size: 16px;
		width:95%;
	}
	thead {
		background-color: #42753C;
		color: white;
	}
</style>
<script>
$(document).ready(function(){
	$("#tabs li").removeClass();
	$("#settingsTab").addClass("active");
	getList();
	$("#appForm").submit(function() {
		var name = $('#appForm #appName').val().trim();
		//var poolName = $('#appForm #pool').val().trim();
		//var machine = $('#appForm #machine').val().trim();
		//var port = $('#appForm #port').val().trim();
		appConfigOperation(name, "updateApp",
				constructAppConfigJson(name)); 
						//machine, port));
		return false;
	});
});

function getList(){
	$.ajax({
		url : "settings/listAllApp",
		type : "GET",
		contentType : "application/json",
		dataType : "json",
		async : false
	}).success(function(data) {
		checkLogin(data);
		$('#appConfigTableBody').empty();
		if(data.success){
			for(var index in data.payload) {
				var rowData = data.payload[index];
				onerow = [];
				onerow.push('<td>' + rowData.name + "</td>");
				//onerow.push('<td>' + rowData.poolName + "</td>");
				//onerow.push('<td>' + rowData.machine + "</td>");
				//onerow.push('<td>' + rowData.port + "</td>");
				onerow.push('<td style="text-align: center; white-space:nowrap;">'
						+ '<i class="icon-edit" title="Modify" href="#appDiv" data-toggle="modal"></i>&nbsp; '
						+ '<i class="icon-trash" title="Delete"></i>'
						+ '</td>');
				$('#appConfigTableBody').append('<tr>' + onerow.join('') + '</tr>');
			}
			$("#appConfigTableBody i").css("cursor", "pointer");
			$("#appConfigTableBody .icon-edit").bind('click', setAppData);
			$("#appConfigTableBody .icon-trash").bind('click', deleteAppConfig);
		}else{
			$("#settingsMsg").text(data.message);
			$("#settingsMsg").show();
		}
		
	}).error({});
}

function appConfigOperation(app, func, jsonStr) {
	$("#appFormMsg").hide();
	$.ajax({
		url : "settings/" + func,
		type : "POST",
		contentType : "application/json",
		dataType : "json",
		data : jsonStr,
	}).success(function(data) {
		checkLogin(data);
		if (!data.success) {
			$('#appFormMsg').html(data.message);
			$('#appFormMsg').removeClass("alert-success");
			$('#appFormMsg').addClass("alert-error");
			$("#appFormMsg").show();
		} else {
			$('#appFormMsg').html(func + " successfully!")
			$('#appFormMsg').addClass("alert-success");
			$('#appFormMsg').removeClass("alert-error");
			$("#appFormMsg").show();
			setTimeout("$(fadeAndReload('appDiv'))", 1000);
		}
	}).error(function(jqXHR, textStatus, errorThrown) {
		$('#resultMessage').html(errorThrown)
		$('#resultMessage').addClass("alert-error");
		$('#resultMessage').removeClass("alert-success");
	});
}

function fadeAndReload(modalName) {
	$('#' + modalName).modal('hide');
	window.location.href="settings";
}

function setAppData() {
	var tr = this.parentNode.parentNode;
	$("#appFormMsg").hide();
	$("#appDiv #appName").val($(tr).find('td').eq(0).text());
	//$("#appDiv #pool").val($(tr).find('td').eq(1).text());
	//$("#appDiv #machine").val($(tr).find('td').eq(1).text());
	//$("#appDiv #port").val($(tr).find('td').eq(2).text());
}

function deleteAppConfig(){
	if (confirm("Are you sure you want to delete?")) {
		var tr = this.parentNode.parentNode;
		var name = $(tr).find('td').eq(0).text();
		//var poolName = $(tr).find('td').eq(1).text();
		//var machine = $(tr).find('td').eq(1).text();
		//var port = $(tr).find('td').eq(2).text();
		appConfigOperation(name, "deleteApp",
				constructAppConfigJson(name)); 
						//machine, port));
	}
}
</script>
</head>
<body>
	<jsp:include page="header.jsp" flush="true"/>
	<div class="alert alert-error" id="settingsMsg"
				style="display: none;text-align: center;font-weight: bold;font-size: 17px;"></div>
	<form class="form-horizontal container" method="post" id="form">
		<h1 style="text-align: center;">Settings</h1>
		<table id="appConfigTable" class="table table-bordered table-striped table-hover">
			<thead>
				<tr>
					<th>App Name</th>
					<!-- 
					<th>Machine</th>
					<th>Port</th>
					 -->
					<th>Action</th>
			</thead>
			<tbody id="appConfigTableBody">
			</tbody>
		</table>
	</form>
	<jsp:include page="sub-views/AppConfig.jsp" flush="true"/>
</body>
</html>
