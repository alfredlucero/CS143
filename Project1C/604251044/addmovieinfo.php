<html>
	<body>
		<?php include("homeui.php"); ?>
		
		<div ="main">
			Add new movie: <br />
			<form action="addmovieinfo.php" method="GET">
				Title: <input type="text" name="title">
				<br />
				Year: <input type="number" name="year">
				<br />
				Rating: <select name='rating'>
						<option value='G'>G</option>
						<option value='PG'>PG</option>
						<option value='PG-13'>PG-13</option>
						<option value='R'>R</option>
						</select> 
				<br />
				Company: <input type="text" name="company">
				<br />
				Genre:
				<input type="checkbox" name="genre[]" value="Action">Action
				<input type="checkbox" name="genre[]" value="Adult">Adult
				<input type="checkbox" name="genre[]" value="Adventure">Adventure
				<input type="checkbox" name="genre[]" value="Animation">Animation
				<input type="checkbox" name="genre[]" value="Comedy">Comedy
				<input type="checkbox" name="genre[]" value="Crime">Crime
				<input type="checkbox" name="genre[]" value="Documentary">Documentary
				<input type="checkbox" name="genre[]" value="Drama">Drama
				<input type="checkbox" name="genre[]" value="Family">Family
				<input type="checkbox" name="genre[]" value="Fantasy">Fantasy
				<input type="checkbox" name="genre[]" value="Horror">Horror
				<input type="checkbox" name="genre[]" value="Musical">Musical
				<input type="checkbox" name="genre[]" value="Mystery">Mystery
				<input type="checkbox" name="genre[]" value="Romance">Romance
				<input type="checkbox" name="genre[]" value="Sci-Fi">Sci-Fi
				<input type="checkbox" name="genre[]" value="Short">Short
				<input type="checkbox" name="genre[]" value="Thriller">Thriller
				<input type="checkbox" name="genre[]" value="War">War
				<input type="checkbox" name="genre[]" value="Western">Western
				<br />
				<input type="submit" name="AddMovieInfoButton" value="AddMovieInfo"><br />
				<hr>
			</form>
			
			<?php
				if ($_GET['AddMovieInfoButton']) {
					if ($_GET['genre'] && $_GET['title'] && $_GET['year'] && $_GET['rating'] && $_GET['company']) {
						// Retrieve input for Movie and MovieGenre tuples
						$id;
						$genres = $_GET['genre'];
						$year = $_GET['year'];
						$title = $_GET['title'];
						$rating = $_GET['rating'];
						$company = $_GET['company'];
						
						// Establish connection and choose CS143 database
						$db_connection = mysql_connect("localhost", "cs143", "");
						mysql_select_db("CS143", $db_connection);
								
						$update_id_query = "UPDATE MaxMovieID SET id = id + 1;";
						$select_id_query = "SELECT id FROM MaxMovieID;";
								
						// Run query on database to update MaxMovieID
						$update_id_results = mysql_query($update_id_query, $db_connection);
						$select_id_results = mysql_query($select_id_query, $db_connection);
					
						$id_row = mysql_fetch_row($select_id_results);
						$id = $id_row[0];
								
						$insert_movie_query = "INSERT INTO Movie VALUES(" . $id . ", '" . $title . "', " . $year . ", '" . $rating . "', '" . $company . "');";
						echo "$insert_movie_query<br />";
								
						// Run query on database to insert into Movie and MovieGenre
						$insert_movie_results = mysql_query($insert_movie_query);
						
						foreach ($genres as $genre) {
							$insert_genre_query = "INSERT INTO MovieGenre VALUES(" . $id . ", '" . $genre . "');";
							$insert_genre_results = mysql_query($insert_genre_query);
						}
						
						mysql_close($db_connection);
									
						echo "<h2>Successfully added a movie to menteMDB!</h2>"; 
					} else { // Not all forms properly filled
						echo "<h2>Didn't add a movie to menteMDB! Please fill in all the fields properly!</h2>";
					}
				}
			?>
		</div>
		
		<?php include("footer.php");?>
	</body>
</html>