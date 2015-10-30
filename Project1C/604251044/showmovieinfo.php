<html>
	<body>
		<?php include("homeui.php"); ?>

		<div ="main">
			<?php
				$id=$_GET['id'];
				$movie_query = "SELECT DISTINCT *
								FROM Movie
								WHERE id = $id;";

				$mdirector_query = "SELECT DISTINCT D.first, D.last, D.dob, D.dod
									FROM MovieDirector MD, Movie M, Director D
									WHERE MD.mid = $id AND M.id = $id AND D.id = MD.did;";

				$mgenre_query = "SELECT genre
								 FROM MovieGenre
								 WHERE mid = $id;";			

				$mactor_query = "SELECT DISTINCT A.first, A.last, A.id, MA.role
								 FROM MovieActor MA, Movie M, Actor A
								 WHERE MA.mid = $id AND M.id = $id AND A.id = MA.aid;";

				// Connect to CS143 database
				$db_connection = mysql_connect("localhost", "cs143", "");
				mysql_select_db("CS143", $db_connection);
						
				// Run queries on database
				$movie_results = mysql_query($movie_query, $db_connection);		
				$mdirector_results = mysql_query($mdirector_query, $db_connection);
				$mgenre_results = mysql_query($mgenre_query, $db_connection);
				$mactor_results = mysql_query($mactor_query, $db_connection);

				while ($movie_row = mysql_fetch_row($movie_results)) {
					$title = $movie_row[1];
					$year = $movie_row[2];
					$rating = $movie_row[3];
					$company = $movie_row[4];
				}

				echo "-- Movie Information --<br><br>";
				echo "Title: ".$title." (".$year.")<br>";
				echo "Production Company: ".$company."<br>";
				echo "MPAA Rating: ".$rating."<br>";
				echo "Director(s): ";

				while ($mdirector_row = mysql_fetch_row($mdirector_results)) {
					$dfirst = $mdirector_row[0];
					$dlast = $mdirector_row[1];
					$ddob = $mdirector_row[2];
					$ddod = $mdirector_row[3];
					echo $dfirst." ".$dlast." (".$ddob." - ".$ddod.")   ";
				}

				echo "<br>Genre: ";

				while ($mgenre_row = mysql_fetch_row($mgenre_results)) {
					$genre = $mgenre_row[0];
					echo $genre."  ";
				}

				echo "<br><br>-- Actors in this Movie --<br><br>";

				while ($mactor_row = mysql_fetch_row($mactor_results)) {
					$afirst = $mactor_row[0];
					$alast = $mactor_row[1];
					$aid = $mactor_row[2];
					$role = $mactor_row[3];
					echo '<a href="showactorinfo.php?id='.$aid.'">'.$afirst." ".$alast."</a> as \"".$role."\"<br>";
				}

				mysql_close($db_connection);	
			?>
		</div>
		
		<?php include("footer.php");?>
	</body>
</html>
