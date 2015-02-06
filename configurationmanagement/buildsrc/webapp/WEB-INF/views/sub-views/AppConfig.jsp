<div id="appDiv" class="modal hide fade" style="width:70%;left:35%;top:6%;" data-backdrop="static">
	<form id="appForm" method="post">
		<div class="modal-body" style="max-height: 470px;">
			<div class="alert alert-success" id="appFormMsg"
				style="display: none;text-align: center;font-weight: bold;font-size: 17px;"></div>
			<div class="form-horizontal" id="appInputDiv">
				<div class="control-group">
					<label class="control-label" for="appName">App Name*</label>
					<div class="controls">
						<input type="text" id="appName" name="appName" required>
					</div>
				</div>
				<!-- 
				<div class="control-group">
					<label class="control-label" for="machine">Machine*</label>
					<div class="controls">
						<input type="text" id="machine" name="machine" required>
					</div>
				</div>
				<div class="control-group">
					<label class="control-label" for="port">Port*</label>
					<div class="controls">
						<input type="text" id="port" name="port" required
							onkeypress="return validateNumber(event, true)">
					</div>
				</div>
				 -->
			</div>
		</div>
		<div class="modal-footer" id="appFooterDiv" style="text-align: center">
			<button class="btn btn-primary" type="submit" onclick="return validateAppConfigInputs();">Save</button>
			<button class="btn" data-dismiss="modal" id="cancelBtn">Cancel</button>
		</div>
	</form>
</div>

<script>
function validateAppConfigInputs(){
	$("#appForm #appFormMsg").css("display", "none");
	if( $("#appForm #machine").val().trim() == ""){
		$("#appForm #appFormMsg").html("Please input machine name.");
		$("#appForm #appFormMsg").removeClass("alert-success");
		$("#appForm #appFormMsg").addClass("alert-error");
		$("#appForm #appFormMsg").css("display", "block");
		return(false);
	}
	return true;
}

function initAppConfigInputs(){
	emptyInputs("appDiv");
	$("#appForm #appFormMsg").css("display", "none");
}
</script>