import pandas as pd
from sqlalchemy import create_engine

# Шлях до CSV-файлу
csv_constructors_path = "constructors.csv"
csv_drivers_path = "drivers.csv"
csv_races_path = "races.csv"
csv_results_path = "results.csv"

# Завантажити дані з CSV-файлу в Pandas DataFrame
constructors_data = pd.read_csv(csv_constructors_path)
drivers_data = pd.read_csv(csv_drivers_path)
races_data = pd.read_csv(csv_races_path)
results_data = pd.read_csv(csv_results_path)

# Перейменувати стовпець 'id' на 'constructors_id'
constructors_data = constructors_data.rename(columns={'id': 'constructors_id'})
drivers_data = drivers_data.rename(columns={'id': 'drivers_id'})
races_data = races_data.rename(columns={'id': 'races_id'})
results_data = results_data.rename(columns={'id': 'results_id'})

# Список умов
id_drivers = [830, 815, 4, 832, 1, 840, 847, 822, 842, 848, 852, 858, 825, 856, 807, 855, 846, 839, 844, 857]
id_constructors = [1, 3, 6, 9, 51, 117, 131, 210, 213, 214]
id_race = [1098, 1099, 1100, 1101, 1102, 1103, 1104, 1105, 1106, 1107, 1108, 1109, 1110, 1111, 1112, 1113, 1114, 1115, 1116, 1117, 1118, 1119, 1120]
race_year = 2023

# Вибрати конкретні стовпці для подальшого запису
constructors_selected_columns = ['constructorId', 'name']
drivers_selected_columns = ['driverId', 'number', 'forename', 'surname', 'nationality']
races_selected_columns = ['raceId', 'year', 'name']
results_selected_columns = ['resultId', 'raceId', 'driverId', 'constructorId', 'position', 'points']

# Відфільтрувати дані за id
constructors_filtered_data = constructors_data[constructors_data['constructorId'].isin(id_constructors)]
drivers_filtered_data = drivers_data[drivers_data['driverId'].isin(id_drivers)]
races_filtered_data = races_data[races_data['year'] == 2023]
results_filtered_data = results_data[results_data['raceId'].isin(id_race)]

# Обрати лише певні стовпці
constructors_filtered_data = constructors_filtered_data[constructors_selected_columns]
drivers_filtered_data = drivers_filtered_data[drivers_selected_columns]
races_filtered_data = races_filtered_data[races_selected_columns]
results_filtered_data = results_filtered_data[results_selected_columns]

# З'єднання з базою даних PostgreSQL
database_url = 'postgresql://postgres:111@localhost:5432/f1'
engine = create_engine(database_url)

# Записати відфільтровані та вибрані стовпці дані в базу даних PostgreSQL
constructors_filtered_data.to_sql('constructors', engine, if_exists='replace', index=False)
drivers_filtered_data.to_sql('drivers', engine, if_exists='replace', index=False)
races_filtered_data.to_sql('races', engine, if_exists='replace', index=False)
results_filtered_data.to_sql('results', engine, if_exists='replace', index=False)

# Закрити з'єднання
engine.dispose()
