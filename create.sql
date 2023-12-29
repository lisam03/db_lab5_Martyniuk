CREATE TABLE drivers
(
  driverId INT NOT NULL,
  number INT,
  forename VARCHAR(100) NOT NULL,
  surname VARCHAR(100) NOT NULL,
  nationality VARCHAR(100),
  PRIMARY KEY (driver_id)
);

CREATE TABLE constructors
(
  constructorId INT NOT NULL,
  name VARCHAR(100) NOT NULL,
  PRIMARY KEY (constructor_id)
);

CREATE TABLE races
(
  raceId INT NOT NULL,
  year INT NOT NULL,
  name VARCHAR(100) NOT NULL,
  PRIMARY KEY (race_id)
);

CREATE TABLE results
(
  resultId INT NOT NULL,
  raceId INT NOT NULL,
  driverId INT NOT NULL,
  constructorId INT NOT NULL,
  position INT NOT NULL,
  points INT NOT NULL,
  PRIMARY KEY (result_id),
  FOREIGN KEY (constructor_id) REFERENCES constructors(constructor_id),
  FOREIGN KEY (driver_id) REFERENCES drivers(driver_id),
  FOREIGN KEY (race_id) REFERENCES races(race_id)
);










