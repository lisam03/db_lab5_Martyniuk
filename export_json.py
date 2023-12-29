import pandas as pd
from sqlalchemy import create_engine
import json

def export_all_tables_to_json(engine):
    # З'єднання з базою даних PostgreSQL
    database_url = 'postgresql://postgres:111@localhost:5432/f1'
    engine = create_engine(database_url)

    # Список таблиць бази даних
    tables = ['constructors', 'drivers', 'races', 'results']

    # Створити порожній словник для збереження даних
    data_dict = {}

    # Експортувати дані для кожної таблиці
    for table in tables:
        # Зчитати дані з таблиці у Pandas DataFrame
        query = f"SELECT * FROM {table};"
        df = pd.read_sql(query, engine)

        # Конвертувати DataFrame у словник і додати його до загального словника
        data_dict[table] = df.to_dict(orient='records')

    # Зберегти дані у JSON-файл
    json_filename = "all_data.json"
    with open(json_filename, 'w') as json_file:
        json.dump(data_dict, json_file, indent=4)

    print(f"Дані з усіх таблиць збережено у {json_filename}")

    # Закрити з'єднання
    engine.dispose()

if __name__ == "__main__":
    # З'єднання з базою даних PostgreSQL
    database_url = 'postgresql://postgres:111@localhost:5432/f1'
    engine = create_engine(database_url)

    # Викликати функцію для експорту всіх таблиць у JSON
    export_all_tables_to_json(engine)
