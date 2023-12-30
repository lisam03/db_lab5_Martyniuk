import csv
import psycopg2
from psycopg2 import sql

# З'єднання з базою даних PostgreSQL
username = 'postgres'
password = '111'
database = 'f1'
host = 'localhost'
port = '5432'

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

# Створення курсора
cur = conn.cursor()

# Шлях до CSV-файлу
csv_constructors_path = "D:\КПІ\Вступ до баз даних\lr5\data\constructors.csv"
csv_drivers_path = "D:\КПІ\Вступ до баз даних\lr5\data\drivers.csv"
csv_races_path = "D:\КПІ\Вступ до баз даних\lr5\data\\races.csv"
csv_results_path = "D:\КПІ\Вступ до баз даних\lr5\data\\results.csv"

clear_query = '''
DELETE FROM results;
DELETE FROM constructors;
DELETE FROM drivers;
DELETE FROM races;
'''

cur.execute(clear_query)

# Відкриття CSV-файлу та імпорт даних в кожну таблицю drivers
with open(csv_drivers_path, 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # Пропустити заголовок CSV-файлу
    for row in csv_reader:
        # Вибірка даних зі строки для подальшої вставки
        driverId, driverRef, number, code, forename, surname, dob, nationality, url = row

        # Вставка даних в таблицю
        cur.execute("INSERT INTO drivers (driver_id, driver_forename, driver_surname, driver_number, driver_nationality) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (driver_id) DO NOTHING",
                    (driverId, forename, surname, number, nationality))

# Відкриття CSV-файлу та імпорт даних в кожну таблицю constructors
with open(csv_constructors_path, 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # Пропустити заголовок CSV-файлу
    for row in csv_reader:
        # Вибірка даних зі строки для подальшої вставки
        constructorId, constructorRef, name, nationality, url = row

        # Вставка даних в таблицю
        cur.execute("INSERT INTO constructors (constructor_id, constructor_name) VALUES (%s, %s) ON CONFLICT (constructor_id) DO NOTHING",
                    (constructorId, name))

# Відкриття CSV-файлу та імпорт даних в кожну таблицю races
with open(csv_races_path, 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # Пропустити заголовок CSV-файлу
    for row in csv_reader:
        # Вибірка даних зі строки для подальшої вставки
        raceId, year, round, circuitId, name, date, time, url, fp1_date, fp1_time, fp2_date, fp2_time, fp3_date, fp3_time, quali_date, quali_time, sprint_date, sprint_time = row

        # Вставка даних в таблицю
        if int(raceId) >= 1098:
            cur.execute("INSERT INTO races (race_id, race_year, race_name) VALUES (%s, %s, %s) ON CONFLICT (race_id) DO NOTHING",
                        (raceId, year, name))

# Відкриття CSV-файлу та імпорт даних в кожну таблицю results
with open(csv_results_path, 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader)  # Пропустити заголовок CSV-файлу
    for row in csv_reader:
        # Вибірка даних зі строки для подальшої вставки
        resultId, raceId, driverId, constructorId, number, grid, position, positionText, positionOrder, points, laps, time, milliseconds, fastestLap, rank, fastestLapTime, fastestLapSpeed, statusId = row

        # Додайте умову, щоб зберігалися лише значення result_id, які починаються з 25846
        if int(raceId) >= 1098:
            cur.execute(
                "INSERT INTO results (result_id, race_id, driver_id, constructor_id, final_position, points) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (result_id) DO NOTHING",
                (resultId, raceId, driverId, constructorId, position, points))


# Збереження змін та закриття курсора та з'єднання
conn.commit()
cur.close()
conn.close()
