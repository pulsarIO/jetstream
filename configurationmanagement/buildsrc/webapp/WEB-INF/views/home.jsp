<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>JetStream Configuration Management</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="resources/css/bootstrap.css" rel="stylesheet">
<link href="resources/css/jquery.dataTables.css" rel="stylesheet">
<script src="resources/js/jquery-1.10.2.js"></script>
<script src="resources/js/bootstrap.js"></script>
<script src="resources/js/jquery.dataTables.js"></script>
<script src="resources/js/configuration.js"></script>
<script src="resources/js/JetStreamCommon.js"></script>
<script src="resources/js/Base64.js"></script>
<script type="text/javascript" src="resources/js/jquery-ui-1.9.2-min.js"></script>
<script type="text/javascript" src="resources/js/jquery.jsPlumb-1.5.4.js"></script>
<style>
	thead {
		background-color: #42753C;
		color: white;
	}
	label {
		font-weight: bold;
	}
	body {
        padding-top: 60px;
        padding-bottom: 40px;
	}
</style>
</head>
<body>
	<jsp:include page="header.jsp" flush="true"/>
	<form class="form-horizontal container">
		<h1 style="text-align: center;">JetStream Configuration
			Management</h1>
		<div class="alert alert-success" id="alertMessage"
					style="display: none;text-align: center;font-weight: bold;font-size: 17px;"></div>
		<div class="control-group" style="margin-top: 22px;margin-bottom: 9px;">
			<input type="button" class="btn btn-primary" id="batchUpdateBtn" value="Update"></input>
			<input type="button" class="btn btn-primary" id="batchDeleteBtn" value="Delete"></input>
			<input type="button" class="btn btn-primary" id="createBtn" href="#dataDiv"
					data-toggle="modal" value="Create"></input>
			<input type="button" class="btn btn-primary" id="fileBtn" value="Import From File"></input>
			<input type="button" class="btn btn-primary" id="logBtn" value="Bean Log"></input>
			<div class="control-group" style="float:right;width:60%;margin-bottom:5px;">
				<label class="control-label" for="appNameSelector" style="margin-right: 10px;font-weight:bold;">App Name</label>
				<div class="controls" style="margin-left: 10px;">
					<select class="input-large" id="appNameSelector">
						<option>All</option>
							<%
								String[] apps = (String[]) request.getAttribute("appNameList");
								String appNameSelected = (String)request.getParameter("appNameSelected");
								if (apps == null) {
									apps = new String[0];
								}
								for (String app : apps) {
									if (appNameSelected != null && appNameSelected.equals(app)) {
							%>
										<option id="<<%=app%>>" selected=true><%=app%></option>
									<%} else {%>
										<option id="<<%=app%>>"><%=app%></option>
									<% } %>
							<%}%>
					</select>
					<a href="#appDiv" data-toggle="modal" id="addAppNameBtn">can't find your app name?</a>
					<!-- 
					<input type="button" class="btn btn-info" id="diagramBtn" value="View Data Flow" disabled></input>
					 -->
				</div>
			</div>
		</div>
		<table id="jetStreamTable" style="width: 100%;"
			class="table table-bordered table-striped table-hover">
			<thead>
				<tr>
					<th><input type="checkbox" id="jetStreamTableCheck"></th>
					<th>App Name</th>
					<th>Version</th>
					<th>Bean Name</th>
					<th>Bean Version</th>
					<th>Scope</th>
					<th>Created By</th>
					<th>Creation Date</th>
					<th>Modified By</th>
					<th>Modification Date</th>
					<th>Bean Definition</th>
					<!-- <th>Action</th> -->
			</thead>
			<tbody id="jetStreamTableBody">
			</tbody>
		</table>
		<div style="text-align:center;">
			<img src="../resources/img/loading.gif" id="loading" style="padding-top: 50px;display:none"></img>
		</div>
		<a href="#" download="jetStreamConfig.xml" id="exportLink" ></a>
	</form>
	
	<input type="file" id="chooseFileBtn" style="display:none;"></input>

	<div id="dataDiv" class="modal hide fade" style="width:70%;left:35%;top:6%;" data-backdrop="static">
		<form id="dataForm" method="post">
			<div class="modal-body" style="max-height: 470px;">
				<div class="alert alert-success" id="resultMessage"
					style="display: none;text-align: center;font-weight: bold;font-size: 17px;"></div>
				<div class="form-horizontal" id="dataInputDiv">
					<div class="span4">
						<div class="control-group">
							<label class="control-label" for="appName">App Name*</label>
							<div class="controls">
								<select class="input-large" id="appName">
										<%
											for (String app : apps) {
										%>
											<option><%=app%></option>
										<%}%>
								</select>
							</div>
						</div>
						<div class="control-group uploadSpec">
							<label class="control-label" for="beanVersion">Bean
								Version</label>
							<div class="controls">
								<input type="text" id="beanVersion" name="beanVersion"
									onkeypress="return validateNumber(event, true)">
							</div>
						</div>
					</div>
					<div class="span4">
						<div class="control-group">
							<label class="control-label" for="version">Version*</label>
							<div class="controls">
								<input type="text" id="version" name="version"
									onkeypress="return validateNumber(event, false)" required>
							</div>
						</div>
						<div class="control-group uploadSpec" id="beanNameDiv">
							<label class="control-label" for="beanName">Bean Name*</label>
							<div class="controls">
								<input type="text" id="beanName" name="beanName" required>
							</div>
						</div>
					</div>
					<div class="control-group uploadSpec span9">
						<label class="control-label" for="scope">Scope</label>
						<div class="controls" id="scopeDiv">
							<select id="scope">
								<option selected value="global">global</option>
								<option value="dc">dc</option>
								<option value="local">local</option>
							</select>
							<select id="dc" style="display:none;">
								<%
								String[] dataCenters = (String[]) request.getAttribute("dcList");
								for (String dc : dataCenters) {%>
									<option id="<<%=dc%>>"><%=dc%></option>
								<%}%>
							</select>
							<input type="text" style="display:none;" id="local" placeholder="Please input machine name"></input>
							<a href="#settingDCDiv" data-toggle="modal" id="setDCBtn" style="display:none;">can't find the data center?</a>
						</div>
					</div>
					<div class="control-group span9">
						<label class="control-label" for="beanDefinition">Bean
							Definition*</label>
						<div class="controls">
							<textarea id="beanDefinition" name="beanDefinition"
								required style="width: 600px; height: 300px;"></textarea>&nbsp;
							<a href="javascript:window.open('http://www.springframework.org/schema/beans/spring-beans-2.0.xsd')">
								<i class="icon-question-sign" title="How to define a bean?"></i></a>
						</div>
					</div>
				</div>
			</div>
			<div class="modal-footer" id="dataFooterDiv" style="text-align: center">
				<button class="btn btn-info" id="validateBtn" name="validate">Validate</button>
				<input type="button" class="btn btn-info" id="parseBtn" value="Parse" name="parseBeans"/>
				<button class="btn btn-primary" id="saveBtn" type="submit"
					name="create">Save & Apply</button>
				<input type="button" class="btn btn-info" id="exportBtn" value="Export"/>
				<button class="btn" data-dismiss="modal" id="cancelBtn">Cancel</button>
			</div>
		</form>
	</div>
	
	<div id="viewBeanDef" class="modal hide fade" style="width:70%;left:35%;" data-backdrop="static">
		<div class="modal-header" style="text-align: right;">
			<button type="button" data-dismiss="modal" style="margin-right: 5px; float: none;" class="close">&times;</button>
		</div>
		<div class="modal-body">
			<textarea id="viewBeanDefBody" style="width:96%; height: 350px;" disabled></textarea>
		</div>
	</div>
	
	<div id="viewBeanDefInRB" class="modal hide fade" style="width:70%;left:35%;" data-backdrop="static">
		<div class="modal-header">
			 <button type="button" class="close" data-dismiss="modal" aria-hidden="true">×</button>
			<h3 id="beanVersionRB">Bean Version</h3>
		</div>
		<div class="modal-body">
			<textarea id="viewBeanDefBodyInRB" style="width:96%; height: 350px;" readonly></textarea>
		</div>
	</div>
	
	<div id="beansDiv" class="modal hide fade" style="width:70%;left:35%;top:6%;" data-backdrop="static">
		<div class="modal-header">
			<!-- <button type="button" class="close" data-dismiss="modal" aria-hidden="true">&times;</button> -->
			<h3 id="beansLabel">Batch Upload</h3>
		</div>
		<div class="modal-body" style="max-height: 470px;">
			<div class="alert alert-success" id="beansMsg"
				style="display: none;text-align: center;font-weight: bold;font-size: 17px;"></div>
			<div class="form-horizontal" id="dataInputDiv">
				<table id="beansTable" style="width: 100%;"
					class="table table-bordered table-striped table-hover">
					<thead>
						<tr>
							<th>App Name</th>
							<th>Version</th>
							<th>Bean Name</th>
							<th>Bean Version</th>
							<th>Scope</th>
							<th>Bean Definition</th>
							<th style="text-align:center;" id="checkCol"><input type="checkbox" id="checkAll" checked="true"/></th>
					</thead>
					<tbody id="beansTableBody">
					</tbody>
				</table>
				<textarea id="beanDefDetail" style="display:none;width:98%;height:150px;" disabled></textarea>
				<div style="display:none;" id="beansDefEdit"></div>
			</div>
		</div>
		<div class="modal-footer" id="beansFooterDiv" style="text-align: center">
			<button class="btn btn-info" id="valiBeansBtn" type="submit" name="create">Validate</button>
			<button class="btn btn-primary" id="pushBeansBtn" type="submit" name="create">Save & Apply</button>
			<button class="btn btn-info" id="expoBeansBtn" type="submit" name="create">Export</button>
			<button class="btn" data-dismiss="modal">Cancel</button>
		</div>
	</div>
	
	<div id="beansDivRB" class="modal hide fade" style="width:80%;left:28%;top:6%;" data-backdrop="static">
		<div class="modal-header">
			<h3 id="beansLabelRB">Bean History Log</h3>
		</div>
		<div class="modal-body" style="max-height: 470px;">
			<div class="alert alert-success" id="beansMsgRB"
				style="display: none;text-align: center;font-weight: bold;font-size: 17px;"></div>
			<div class="form-horizontal" id="dataInputDiv">
				<table id="beansTableRB" style="width: 100%;"
					class="table table-bordered table-striped table-hover">
					<thead>
						<tr>
							<th>App Name</th>
							<th>Version</th>
							<th>Bean Name</th>
							<th>Bean Version</th>
							<th>Scope</th>
							<th>Created By</th>
							<th>Creation Date</th>
							<th>Modified By</th>
							<th>Modification Date</th>
							<th>Operated By</th>
							<th>Operation Date</th>
							<th>Status</th>
							<th>Bean Definition</th>
					</thead>
					<tbody id="beansTableBodyRB">
					</tbody>
				</table>
			</div>
		</div>
		<div class="modal-footer" id="beansFooterDiv" style="text-align: center">
			<button class="btn" data-dismiss="modal" id = "cancelBtn">Close</button>
		</div>
	</div>
	
	<jsp:include page="sub-views/AppConfig.jsp" flush="true"/>
	
	<div id="settingDCDiv" class="modal hide fade" tabindex="-1"
		role="dialog" aria-labelledby="settingLabel" aria-hidden="true"
		data-backdrop="static">
			<div class="modal-header">
				<button type="button" class="close" data-dismiss="modal"
						aria-hidden="true">&times;</button>
				<h3 id="settingLabel">DC Settings</h3>
			</div>
			<div class="modal-body">
				<div class="alert alert-success" id="setDCMsg"
					style="display: none;text-align: center;font-weight: bold;font-size: 17px;">Update Success!</div>
				<select id="setDCSelector" size=<%=apps.length%>>
					<%
					for (String dc : dataCenters) {%>
						<option id="<<%=dc%>>"><%=dc%></option>
					<%}%>
				</select>
				<textarea style="display:none;" id="dcEdit"></textarea>
			</div>
			<div class="modal-footer">
				<button class="btn btn-primary" id="editDCBtn">Edit</button>
				<button class="btn btn-primary" id="saveDCBtn" name="dc">Save</button>
				<button class="btn" data-dismiss="modal" aria-hidden="true" id="cancelDCBtn">Cancel</button>
			</div>
	</div>

</body>
</html>
