<div class="navbar navbar-inverse navbar-fixed-top">
	<div class="navbar-inner"
		style="box-shadow: 0 1px 3px rgba(0, 0, 0, 0.25), inset 0 -1px 0 rgba(0, 0, 0, 0.1);
		background-image: linear-gradient(to bottom, #2B7E7E, #13494D);
		background-image: -moz-linear-gradient(to bottom, #2B7E7E, #13494D)">
		 <div class="container" style="width: auto;">
	         <ul class="nav" role="navigation">
	         	<li>
	         		<img src="../resources/img/header.png" style="height: 32px; padding: 10px;background-image:linear-gradient(to bottom, #D7DF01, #AEB404);
	         		background-image: -moz-linear-gradient(to bottom,  #D7DF01, #AEB404)"></img>
	         	</li>
	            <li id="configTab"><a href="configuration">Configuration</a></li>
				<li id="settingsTab"><a href="settings">Settings</a></li>
                <li id="adminTab"><a href="administration">Administration</a></li>
	         </ul>
        </div>
	</div>
	<div class="pull-right" style="margin-top: -40px;">
		<div style="margin-right: 10px;">
			<i class="icon-user icon-white"></i>&nbsp; 
				<span style="margin-right: 40px; color: #FFFFFF;" id="userNameSpan">
				<%
					out.print(request.getRemoteUser());
				%>
				</span>
				<a href="/j_spring_security_logout" title="log off">
					<i class="icon-off icon-white"></i>
				</a>
		</div>
	</div>
</div>

<style type="text/css">
	.navbar-inner li{
		line-height: 30px;
		font-size: 17px;
	    font-weight: 600;
	}
	.navbar-inverse .nav .active > a, 
	.navbar-inverse .nav .active > a:hover, 
	.navbar-inverse .nav .active > a:focus,
	.navbar-inverse .nav li.dropdown.open > .dropdown-toggle, 
	.navbar-inverse .nav li.dropdown.active > .dropdown-toggle, 
	.navbar-inverse .nav li.dropdown.open.active > .dropdown-toggle{
		background-color: rgba(17, 17, 17, 0.4);
	}
	.icon-white {
		background-image: url("resources/img/glyphicons-halflings-white.png");
	}
</style>