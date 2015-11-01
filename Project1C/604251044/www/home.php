<html>
	<body>
		
		<?php include("homeui.php"); ?>
		
		<div ="main">
			<h1>menteMDB</h1>
			<p>Welcome to menteMDB!<br />
			   Check out the better version of imdb created by students from CS 143: Database Systems, Fall 2015. <br />
			   Here we used PHP in conjunction with MySQL databases in order to display query results for actors and movies. <br />
			   If one would like a recap of the past iteration of the project, Project1B, one can click on the link below: <br />
			   <?php $link_name = "Project 1B Query"; ?>
			   <a href="query.php"><?php echo $link_name; ?></a>
			   <br />
			   One must input proper SQL queries in order to retrieve information regarding the movie/actor database. <br />
			   Feel free to use as you wish to see how the underlying system works.
			</p>
		</div>
		
		<?php include("footer.php");?>
	</body>
</html>