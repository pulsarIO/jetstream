<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Log in</title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="resources/css/bootstrap.css" rel="stylesheet">
<script src="resources/js/jquery-1.10.2.js"></script>
<script src="resources/js/bootstrap.js"></script>
<style type="text/css">
.form-login {
	max-width: 300px;
	padding: 19px 29px 29px;
	margin: 0 auto 20px;
	background-color: #fff;
	border: 1px solid #e5e5e5;
	-webkit-border-radius: 5px;
	-moz-border-radius: 5px;
	border-radius: 5px;
	-webkit-box-shadow: 0 1px 2px rgba(0, 0, 0, .05);
	-moz-box-shadow: 0 1px 2px rgba(0, 0, 0, .05);
	box-shadow: 0 1px 2px rgba(0, 0, 0, .05);
}

.form-login input[type="text"],.form-login input[type="password"],.form-login img
	{
	font-size: 16px;
	height: auto;
	margin-bottom: 15px;
	padding: 7px 9px;
}

.form-login div button {
	width: 80px;
}
</style>
<script>
		var browser = navigator.userAgent.toUpperCase();
		if (browser.indexOf("MSIE") != -1) {
			
			alert('Please switch to FireFox or Chrome');
			$('*').hide();
		}
	</script>
</head>

<body style="background-color: #f5f5f5;">
	<div class="container" style="padding-top: 40px">
		<form class="form-login" method="post"
			action="/j_spring_security_login">
			<img src="resources/img/JetStreamConfig.jpg"></img>
			<c:if test="${not empty param['error']}">
			      <font color="red">
			        <c:out value="${SPRING_SECURITY_LAST_EXCEPTION.message}"/>.
			      </font>
			</c:if>

			<input type="text" name="j_username" class="input-block-level"
				placeholder="User Name" required> <input type="password"
				name="j_password" class="input-block-level" placeholder="Password"
				required>
			<div style="text-align: center">
				<button class="btn btn-primary" type="submit">Log in</button>
			</div>
		</form>
	</div>
</body>
</html>
