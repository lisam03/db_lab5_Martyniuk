-- Заповнення таблиці drivers
INSERT INTO drivers (driver_id, driver_forename, driver_surname, driver_number, driver_nationality)
VALUES
  (1, 'Lewis', 'Hamilton', 44, 'British'),
  (847, 'George', 'Russel', 63, 'British'),
  (830, 'Max', 'Versteppen', 33, 'Dutch'),
  (815, 'Sergio', 'Perez', 11, 'Mexican'),
  (844, 'Charles', 'Leclerc', 16, 'Monegasque'),
  (832, 'Carlos', 'Sainz', 55, 'Spanish'),
  (846, 'Lando', 'Norris', 4, 'British'),
  (857, 'Oscar', 'Piastri', 81, 'Australian'),
  (840, 'Lance', 'Stroll', 18, 'Canadian'),
  (4, 'Fernando', 'Alonso', 14, 'Spanish');

  -- Заповнення таблиці constructors
INSERT INTO constructors (constructor_id, constructor_name)
VALUES
  (1, 'McLaren'),
  (6, 'Ferrari'), 
  (131, 'Mercedes'),
  (9, 'Red Bull'),
  (117, 'Aston Martin');

  -- Заповнення таблиці races
INSERT INTO races (race_id, race_year, race_name)
VALUES
  (1105, 2023, 'Spanish Grand Prix'),
  (1109, 2023, 'Hungarian Grand Prix'),
  (1104, 2023, 'Monaco Grand Prix'),
  (1106, 2023, 'Canadian Grand Prix'),
  (1102, 2023, 'Miami Grand Prix'),
  (1101, 2023, 'Azerbaijan Grand Prix'),
  (1110, 2023, 'Belgian Grand Prix'),
  (1107, 2023, 'Austrian Grand Prix'),
  (1100, 2023, 'Australian Grand Prix'),
  (1108, 2023, 'British Grand Prix');

  -- Заповнення таблиці results
INSERT INTO results (result_id, race_id, driver_id, constructor_id, final_position, points)
VALUES
  (25966, 1105, 830, 9, 1, 26),
  (26031, 1110, 815, 9, 2, 18),
  (25911, 1101, 1, 131, 6, 8), 
  (25929, 1102, 847, 131, 4, 12), 
  (25989, 1106, 844, 6, 4, 12),
  (26011, 1107, 832, 6, 6, 8),
  (25954, 1104, 846, 1, 9, 2),
  (26050, 1109, 857, 1, 5, 10),
  (26032, 1108, 4, 117, 7, 6),
  (25889, 1100, 840, 117, 4, 12);







