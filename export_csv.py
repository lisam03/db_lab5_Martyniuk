import csv
import psycopg2

# З'єднання з базою даних PostgreSQL
username = 'postgres'
password = '111'
database = 'f1'
host = 'localhost'
port = '5432'

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

# Створення курсора
cur = conn.cursor()


tables = ['constructors', 'drivers', 'races', 'results']

for table in tables:
    # Вибірка даних з таблиці
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()

    # Шлях до CSV-файлу
    csv_file_path = f"{table}.csv"

    # Запис даних у CSV-файл
    with open(csv_file_path, 'w', encoding='utf-8', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        # Запис заголовка
        csv_writer.writerow([desc[0] for desc in cur.description])

        # Запис даних
        csv_writer.writerows(rows)

