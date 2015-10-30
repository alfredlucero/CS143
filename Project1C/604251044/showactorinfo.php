<html>
	<body>
		<?php include("homeui.php"); ?>
		
		<div ="main">
			<?php 
				$id=$_GET['id'];
				$actor_query = "SELECT DISTINCT *
								FROM Actor
								WHERE id = $id;";

				$actedin_query = "SELECT DISTINCT  M.title, MA.role, M.id
								  FROM MovieActor MA, Movie M, Actor A
								  WHERE MA.aid = $id AND A.id = $id AND M.id= MA.mid;";


				// Connect to CS143 database
				$db_connection = mysql_connect("localhost", "cs143", "");
				mysql_select_db("CS143", $db_connection);
						
				// Run queries on database
				$actor_results = mysql_query($actor_query, $db_connection);		
				$actedin_results = mysql_query($actedin_query, $db_connection);

				while ($actor_row = mysql_fetch_row($actor_results)) {
					$last = $actor_row[1];
					$first = $actor_row[2];
					$gender = $actor_row[3];
					$dob = $actor_row[4];
					$dod = $actor_row[5];
				}

				echo "-- Actor Information --<br><br>";
				echo "Name: ".$first." ".$last."<br>";
				echo "Gender: ".$gender."<br>";
				echo "Date of Birth: ".$dob."<br>";
				echo "Date of Death: ".$dod."<br>";

				echo "<br>-- Movie Roles --<br><br>";

				while ($actedin_row = mysql_fetch_row($actedin_results)) {
					$title = $actedin_row[0];
					$role = $actedin_row[1];
					$mid = $actedin_row[2];
					echo "\"".$role."\"".' in <a href="showmovieinfo.php?id='.$mid.'">'.$title.'</a><br>';
				}

				mysql_close($db_connection);				
			?>


		</div>
		
		<?php include("footer.php");?>
	</body>
</html>
