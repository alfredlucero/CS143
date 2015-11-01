<html>
	<body>
		<?php include("homeui.php"); ?>
		
		<div ="main">
			Add new actor/director:<br />
			<form action="addactordir.php" method="GET">
				Identity: 
				<input type="radio" name="actordir" value="Actor">Actor
				<input type="radio" name="actordir" value="Director">Director
				<br />
				First Name: <input type="text" name="first">
				<br />
				Last Name: <input type="text" name="last">
				<br />
				Sex:
				<input type="radio" name="gender" value="Male">Male
				<input type="radio" name="gender" value="Female">Female
				<br />
				Date of Birth: <input type="text" name="dob"> (YYYY-MM-DD)
				<br />
				Date of Death: <input type="text" name="dod"> (YYYY-MM-DD, leave blank if still alive)
				<br />
				<input type="submit" name="AddActorDirButton" value="AddActorDir"><br />
			</form>
			<hr>
			<?php 
				if ($_GET['AddActorDirButton']) {
					if ($_GET['actordir']) { // checks whether or not the user chose an identity
						// Retrieve input fields for Actor tuple
						$first = $_GET["first"];
						$last = $_GET["last"];
						$sex = $_GET["gender"];
						$dob = $_GET["dob"];
						$dod = $_GET["dod"];
						
						// Make sure all fields are properly inputted
						if ($first && $last && $sex && $dob) {
							if ($_GET['actordir'] == "Actor") {	// Add Actor
								// Establish connection and choose CS143 database
								$db_connection = mysql_connect("localhost", "cs143", "");
								mysql_select_db("CS143", $db_connection);
								
								$update_id_query = "UPDATE MaxPersonID SET id = id + 1;";
								$select_id_query = "SELECT id FROM MaxPersonID;";
								
								// Run query on database to update MaxPersonID
								$update_id_results = mysql_query($update_id_query, $db_connection);
								$select_id_results = mysql_query($select_id_query, $db_connection);
					
								$id_row = mysql_fetch_row($select_id_results);
								$id = $id_row[0];
								
								$insert_actor_query = "INSERT INTO Actor VALUES(" . $id . ", '" . $last . "', '" . $first . "', '" . $sex . "', '" . $dob . "', '" . $dod . "');";
								// echo "$insert_actor_query<br />";
								
								// Run query on database to insert into Actor
								$insert_actor_results = mysql_query($insert_actor_query);
								
								mysql_close($db_connection);
							
								echo "<h2>Successfully added an actor to menteMDB!</h2>"; 
							} else { // Add Director 
								// Establish connection and choose CS143 database
								$db_connection = mysql_connect("localhost", "cs143", "");
								mysql_select_db("CS143", $db_connection);
								
								$update_id_query = "UPDATE MaxPersonID SET id = id + 1;";
								$select_id_query = "SELECT id FROM MaxPersonID;";
								
								// Run query on database to update MaxPersonID
								$update_id_results = mysql_query($update_id_query, $db_connection);
								$select_id_results = mysql_query($select_id_query, $db_connection);
					
								$id_row = mysql_fetch_row($select_id_results);
								$id = $id_row[0];
								
								$insert_director_query = "INSERT INTO Director VALUES(" . $id . ", '" . $last . "', '" . $first . "', '" . $dob . "', '" . $dod . "');";
								// echo "$insert_director_query<br />";
								
								// Run query on database to insert into Director
								$insert_director_results = mysql_query($insert_director_query);
								
								mysql_close($db_connection);
							
								echo "<h2>Successfully added a director to menteMDB!</h2>";
							}
						} else {
								echo "<h2>Didn't add an actor or a director to menteMDB! Please fill in all the fields properly!</h2>";
						}	
					} else {
						echo "<h2>Didn't add an actor or a director to menteMDB! Please choose an identity and fill out the fields!</h2>"; 
					}
				}
			?>
				
		</div>
		
		<?php include("footer.php");?>
	</body>
</html>