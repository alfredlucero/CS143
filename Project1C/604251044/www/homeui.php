<html>
	<head>
		<title>MenteMDB</title>
		<style type="text/css">
			#header {
				background-color:#006699;
				color:white;
				text-align:center;
				padding:5px;
			}
			#nav {
				line-height:30px;
				background-color:#efefef;
				height:100%;
				width:200px;
				float:left;
				padding:5px;
			}
			#main {
				float:left;
				padding:10px;
			}
		</style>
	</head>
	
	<body>
		<div id="header">
			<h1>menteMDB</h1>
			<h2><a href="home.php" style="color:white">Home</a></h2>
		</div>
		
		<div id="nav">
			<h2>ADD CONTENT:</h2>
			<ul style="list-style-type:none">
				<li><a href="addactordir.php">Add Actor/Director</a></li>
				<li><a href="addmovieinfo.php">Add Movie Information</a></li>
				<li><a href="addmovieactor.php">Add Movie/Actor</a></li>
				<li><a href="addmoviedir.php">Add Movie/Director</a></li>
			</ul>
			<h2>BROWSE:</h2>
				<ul style="list-style-type:none">
					<li><a href="showactorinfo.php">Show Actor Info</a></li>
					<li><a href="showmovieinfo.php">Show Movie Info</a></li>
				</ul>
			<h2>SEARCH:</h2>
				<ul style="list-style-type:none">
					<li><a href="search.php">Search Actor/Movie</a></li>
				</ul>
		</div>
	</body>
</html>