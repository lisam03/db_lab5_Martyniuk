import pandas as pd
from sqlalchemy import create_engine

def export_table_to_csv(table_name, engine):
    # Зчитати дані з таблиці у Pandas DataFrame
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql(query, engine)

    # Зберегти дані у CSV-файл
    csv_filename = f"{table_name}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"Дані з таблиці {table_name} збережено у {csv_filename}")

def main():
    # З'єднання з базою даних PostgreSQL
    database_url = 'postgresql://postgres:111@localhost:5432/f1'
    engine = create_engine(database_url)

    # Список таблиць для експорту
    tables_to_export = ['constructors', 'drivers', 'races', 'results']

    # Експортувати дані для кожної таблиці
    for table in tables_to_export:
        export_table_to_csv(table, engine)

    # Закрити з'єднання
    engine.dispose()

if __name__ == "__main__":
    main()
