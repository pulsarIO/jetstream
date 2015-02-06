<html>
	<head>
		<title>Tail-based by Web Sockets</title>
		<script type='text/javascript'>
			if (!window.WebSocket)
				alert("WebSocket not supported by this browser");
		
			function $() {
				return document.getElementById(arguments[0]);
			}
			function $F() {
				return document.getElementById(arguments[0]).value;
			}
		
			function getKeyCode(ev) {
				if (window.event)
					return window.event.keyCode;
				return ev.keyCode;
			}

			function getContextPath() {
				var url = window.location.href;
				var begin = url.indexOf("://");
				var end = url.lastIndexOf("/");

				return url.substr(begin, end + 1 - begin);
			}

			function getWSUrl() {
				var url = "ws" + getContextPath() + "websocket/a";
				return url;
			}

			function sendPing(wsObj) {
				wsObj.send("keepalive");
				setTimeout(function() {
					sendPing(wsObj);
				}, 3000);
			}

			var server = {
				connect : function() {
					if (window.MozWebSocket) {
						this._ws = new MozWebSocket(getWSUrl());
					} 
					else if (window.WebSocket) {
						this._ws = new WebSocket(getWSUrl());
					}
					else {
						$.messager.alert('Warning','EPL tool only works on Firefox and Chrome','warning'); 
						return;
					}
					this._ws.onopen = this._onopen;
					this._ws.onmessage = this._onmessage;
					this._ws.onclose = this._onclose;
				},
		
				_onopen : function() {
					sendPing(this._ws);
				},
		
				_send : function(message) {
					if (this._ws)
						this._ws.send(message);
				},
		
				send : function(text) {
					if (text != null && text.length > 0)
						server._send(text);
				},
		
				_onmessage : function(m) {
					if (m.data) {
						if(m.data == 'CLEAN'){
							var messageBox = $('messageBox');
							messageBox.innerHTML = "";
						}
						else if((m.data).substring(0,4) == 'EPL:'){
							var commandBox = $('commandBox');
							commandBox.value = m.data.substring(4);
						}
						else{
							var messageBox = $('messageBox');
							var spanText = document.createElement('span');
							spanText.className = 'text';
							spanText.innerHTML = m.data;
							var lineBreak = document.createElement('br');
							messageBox.appendChild(spanText);
							messageBox.appendChild(lineBreak);
							messageBox.scrollTop = messageBox.scrollHeight
									- messageBox.clientHeight;
						}
					}
				},
		
				_onclose : function(m) {
					this._ws = null;
				}
			};
		</script>
		<style type='text/css'>
			div {
				border: 0px solid black;
			}
			
			div.box,
			textarea.box{
				clear: both;
				width: 80%;
				overflow: auto;
				padding: 4px;
				border-collapse: separate;
				border-radius: 4px;
				float: right;
				text-align: left;
				margin-top: 10px;
			}
			
			div#messageBox {
				height: 50%;
				background-color: #E9EFFA;
				border: 1px solid rgb(189, 191, 248);
			}
			
			textarea#commandBox {
				height: 33%;
				background-color: #E9EFFA;
				border: 1px solid rgb(189, 191, 248);
			}
			
			div#exeDiv {
				width: 80%;
				float: right;
				text-align: left;
				margin-top: 10px;
			}
			
			div.hidden {
				display: none;
			}
			
			span.alert {
				font-style: italic;
			}
			
			div#message{
				width: 80%;
				text-align: center;
			}
			
			input#execute{
				float: right;
				margin-right: 30px;
				font-size: 17px;
			}
			
		</style>
	</head>
	<body style="font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;" onload="server.connect()">
		
		<div id="message">
			<div id='input'>
				<span style="font-size: 20px; font-weight: bold;">This is a demonstration of the jetstream.</span>
			</div>
			<div id='messageBox' class="box"></div>
			<textarea id='commandBox' class="box"></textarea>
			<div id="exeDiv">
				<input id='execute' class='button' type='submit' name='Execute' value='Execute'/>
			</div>
			
			<script type='text/javascript'>
				$('execute').onclick = function(event) {
					var commandBox = $('commandBox');
					server.send(commandBox.value);
				};
			</script>
		</div>
	</body>
</html>