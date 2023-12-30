import json
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

# Отримання списку всіх таблиць
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables = cur.fetchall()

# Створення об'єкта для збереження усіх даних
all_data = {}

# Експорт даних з кожної таблиці у відповідний ключ у JSON-файлі
for table in tables:
    table_name = table[0]

    # Вибірка даних з таблиці
    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()

    # Додавання даних до об'єкта
    all_data[table_name] = []

    for row in rows:
        row_dict = {}
        for i, desc in enumerate(cur.description):
            row_dict[desc[0]] = row[i]
        all_data[table_name].append(row_dict)

# Шлях до JSON-файлу
json_file_path = "all_data.json"

# Запис усіх даних у JSON-файл
with open(json_file_path, 'w', encoding='utf-8') as json_file:
    json.dump(all_data, json_file, indent=2)

# Закриття курсора та з'єднання
cur.close()
conn.close()
