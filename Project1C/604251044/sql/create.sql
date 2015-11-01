CREATE TABLE Movie (
       id INT,
       title VARCHAR(100),
       year INT,
       rating VARCHAR(10),
       company VARCHAR(50),
	   PRIMARY KEY(id), -- every movie has unique mid
	   CHECK (year >= 0) -- year can't be negative number
       ) ENGINE = INNODB;

CREATE TABLE Actor (
       id INT,
       last VARCHAR(20),
       first VARCHAR(20),
       sex VARCHAR(6),
       dob DATE,
       dod DATE,
	   PRIMARY KEY(id), -- every actor has unique aid
	   CHECK (dob < '2015-10-18') -- check actor was born before cut off date of Oct. 18, 2015
       ) ENGINE = INNODB;

CREATE TABLE Director (
       id INT,
       last VARCHAR(20),
       first VARCHAR(20),
       dob DATE,
       dod DATE,
	   PRIMARY KEY(id) -- every director has unique did
       ) ENGINE = INNODB;

CREATE TABLE MovieGenre (
       mid INT,
       genre VARCHAR(20),
	   FOREIGN KEY(mid) references Movie(id) -- references movie id
       ) ENGINE = INNODB;

CREATE TABLE MovieDirector (
       mid INT,
       did INT,
	   FOREIGN KEY(mid) references Movie(id),	-- references movie id
	   FOREIGN KEY(did) references Director(id) -- references director id
       ) ENGINE = INNODB;

CREATE TABLE MovieActor (
       mid INT,
       aid INT,
       role VARCHAR(50),
	   FOREIGN KEY (mid) references Movie(id), -- references movie id
	   FOREIGN KEY (aid) references Actor(id)  -- references actor id
       ) ENGINE = INNODB;

CREATE TABLE Review (
       name VARCHAR(20),
       time TIMESTAMP,
       mid INT,
       rating INT,
       comment VARCHAR(500),
	   FOREIGN KEY (mid) references Movie(id), -- references movie id
	   CHECK(rating >= 0 AND rating <= 5) -- rating from 0 to 5
       ) ENGINE = INNODB;

CREATE TABLE MaxPersonID (
       id INT
       );

CREATE TABLE MaxMovieID (
       id INT
       );
