CREATE TABLE drivers
(
  driver_id INT NOT NULL,
  driver_forename VARCHAR(100) NOT NULL,
  driver_surname VARCHAR(100) NOT NULL,
  driver_number VARCHAR(20) NOT NULL,
  driver_nationality VARCHAR(100),
  PRIMARY KEY (driver_id)
);

CREATE TABLE constructors
(
  constructor_id INT NOT NULL,
  constructor_name VARCHAR(100) NOT NULL,
  PRIMARY KEY (constructor_id)
);

CREATE TABLE races
(
  race_id INT NOT NULL,
  race_year INT NOT NULL,
  race_name VARCHAR(100) NOT NULL,
  PRIMARY KEY (race_id)
);

CREATE TABLE results
(
  result_id INT NOT NULL,
  race_id INT NOT NULL,
  driver_id INT NOT NULL,
  constructor_id INT NOT NULL,
  final_position VARCHAR(20) NOT NULL,
  points FLOAT NOT NULL,
  PRIMARY KEY (result_id),
  FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id),
  FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);

