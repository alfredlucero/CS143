<html>
<body>
<?php include("homeui.php"); ?>

<h1><strong>Movie Database</strong></h1>
<p>Please do not run a complex query here. You may kill the server.
<br /> Type an SQL query in the following box: <br />
Example: SELECT * FROM Actor WHERE id=10;<br /></p>

<form action="query.php" method="GET">
	<textarea name="query" rows="10" cols="65"></textarea><br />
	<input type="submit" name="submit" value="Submit"><br />
</form>
<p>Note: tables and fields are case sensitive. All tables in Project 1B are available.</p>

<?php
	if($_GET["query"]) {
		// Retrieve input query from text area
		$query = $_GET["query"];
		
		// Establish connection and choose CS143 database
		$db_connection = mysql_connect("localhost", "cs143", "");
		mysql_select_db("CS143", $db_connection);
		
		// Run query on database and keep track of number of fields
		$results = mysql_query($query, $db_connection);
		$num_fields = mysql_num_fields($results);
		
		echo "<h2>Results from MySQL:</h2>";
		// Put query results in a table
		// Set headers of fields
		
		echo "<table border='1'><tr>";
		for ($i = 0; $i < $num_fields; $i++) {
			$field = mysql_fetch_field($results);
			echo "<th>$field->name</th>";
		}
		echo "</tr>";
		
		while($row = mysql_fetch_row($results)) {
			$col_num = 0;
			echo "<tr>";
			while ($col_num < $num_fields) {
				$field_n = $row[$col_num];
				echo "<td>$field_n</td>";
				$col_num++;
			}
			echo "</tr>";
		}
		echo "</table>";
	}
?>
<?php include("footer.php"); ?>
</body>
</html>
