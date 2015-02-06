<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Administration</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<script src="resources/js/jquery-1.10.2.js"></script>

<link href="../resources/css/bootstrap.css" rel="stylesheet">
<link href="../resources/css/jquery-ui-1.9.2.css" rel="stylesheet">
<link href="../resources/css/bootstrap-datetimepicker.css"
	rel="stylesheet">
<script src="resources/js/JetStreamCommon.js"></script>
<script src="resources/js/bootstrap-datetimepicker.js"></script>
<script src="resources/js/bootstrap.js"></script>


<style>
body {
	padding-top: 80px;
	padding-bottom: 40px;
}

label {
	font-weight: bold;
}
</style>
<script>
	$(document).ready(function() {
		$("#tabs li").removeClass();
		$("#adminTab").addClass("active");
		$("#purgeBtn").bind("click", purgeBeanLog);
		$("#purgeDate").bind("focus",setDTPicker);   
	});
	
	function setDTPicker(){
		$('#purgeDate').datetimepicker({
	        weekStart: 1,
	        todayBtn:  1,
			autoclose: 1,
			todayHighlight: 1,
			startView: 2,
			minView: 2,
			forceParse: 1,
			format:'yyyy-mm-dd'
	    });
		$('#purgeDate').datetimepicker('show');
	}
	
	function purgeBeanLog(){
		
		
		var date = $("#purgeDate").val();
		if(date==""){
			alert("Please input date time");
			return;
		}
		
		var re = new RegExp("[0-9]{4}-[0-9]{2}-[0-9]{2}");
		r = re.test(date);
		if(!r){
			alert("Please input the date in format yyyy-MM-dd");
			return;
		}
		
		if(!confirm("Do you want to delete the bean log before " + date+"?")){
			return;
		}
		
		
		$.post('administration/purgeBeanLog',{
			purgeDate:$('#purgeDate').val()
		}).done(function() {
			alert("Purge Successfully!");
			//window.location = 'administration';
		}).fail(function() {
			alert("Purge Failed!");
		});
		
	}
	

</script>
</head>
<body>
	<jsp:include page="header.jsp" flush="true" />
	<div class="container" id="content">
		<a href="javascript:hideOrDisplay('purgeDiv')"><legend>Purge
				Bean Log</legend></a>
		<div id="purgeDiv">
			<div class="control-group">
				<form class="form-horizontal">
					<label class="control-label" for="date" style="width:250px">Purge bean log history before&nbsp</label>
					<div class="controls">		   
						<input type="text" value="" id="purgeDate" name="purgeDate" placeholder="yyyy-mm-dd" required>
						<input type="button" class="btn btn-primary" id="purgeBtn"
							value="Purge"></input>
					</div>
				</form>
			</div>

		</div>
	</div>
</body>
</html>