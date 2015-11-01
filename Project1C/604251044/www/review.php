<html>
	<body>
		<?php include("homeui.php"); ?>
		
		<div ="main">
			Add a review about a movie: <br />
			<form action="review.php" method="GET">
				Movie:
					   <?php
							// Establish connection and choose CS143 database
							$movie = "SELECT title, id FROM Movie ORDER BY title ASC;";
							$db_connection = mysql_connect("localhost", "cs143", "");
							mysql_select_db("CS143", $db_connection);
							
							// Run query on database and keep track of number of fields
							$results = mysql_query($movie, $db_connection);
							
							// Put results into each option select
							echo "<select name='movies'>";
							while($row = mysql_fetch_row($results)) {
								$field1 = $row[0];
								$field2 = $row[1];
								echo "<option value='$field2'>".$field1."</option>";
							}
							echo  "</select>";
							mysql_close($db_connection);
					   ?>
				<br />
				Your Name: <input type="text" name="name">
				<br />
				Rating: <select name="rating">
							<option value="1">1 Star out of 5</option>
							<option value="2">2 Stars out of 5</option>
							<option value="3">3 Stars out of 5</option>
							<option value="4">4 Stars out of 5</option>
							<option value="5">5 Stars out of 5</option>
					    </select>
				<br />
				Comments:
				<br />
				<textarea name="comment" rows="10" cols="65"></textarea>
				<br />
				<input type="submit" name="AddReviewButton" value="AddReview"><br />
				<hr>
			</form>

		</div>
		<?php
			if ($_GET['AddReviewButton']) {
				$db_connection = mysql_connect("localhost", "cs143", "");
					mysql_select_db("CS143", $db_connection);

					if ($_GET['name']) 
						$name = $_GET['name'];
					else
						$name = "Anonymous";

					$mid = $_GET['movies'];
					$rating = $_GET['rating'];
					$comment = $_GET['comment'];

					$time_query = "SELECT NOW();";
					$time_results = mysql_query($time_query);

					$time_row = mysql_fetch_row($time_results);
					$time = $time_row[0];

					$insert_review_query = "INSERT INTO Review VALUES('" . $name . "', '" . $time . "', " . $mid . ", " . $rating . ", '" . $comment . "');";
					
					//Debugging
					//echo $insert_review_query."<br>";
					//echo $time;

					$insert_review_results = mysql_query($insert_review_query);

					echo "<h2>Successfully added review!</h2>"; 
					mysql_close($db_connection);
				}
			?>
		<?php include("footer.php");?>
	</body>
</html>
