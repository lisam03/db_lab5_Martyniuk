import psycopg2
import matplotlib.pyplot as plt


username = 'postgres'
password = '111'
database = 'f1'
host = 'localhost'
port = '5432'


query_1 = '''
SELECT * FROM TeamStandings;
'''

query_2 = '''
SELECT * FROM DriversNatoinality;
'''

query_3 = '''
SELECT * FROM FinalPositionPoints;
'''

create_view_1 = '''
CREATE OR REPLACE VIEW TeamStandings AS
SELECT name, SUM(points) AS TotalPoints
	FROM constructors NATURAL JOIN results
GROUP BY name
ORDER BY TotalPoints DESC;
'''

create_view_2 = '''
CREATE OR REPLACE VIEW DriversNatoinality AS
SELECT nationality, COUNT(forename) AS Total
	FROM drivers
GROUP BY nationality;
'''

create_view_3 = '''
CREATE OR REPLACE VIEW FinalPositionPoints AS
SELECT DISTINCT position as Position, points
	FROM results
GROUP BY position, points
ORDER BY points DESC;
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    cur.execute(create_view_1)
    cur.execute(create_view_2)
    cur.execute(create_view_3)

    figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3, figsize=(15, 5))

    """
    Перший запит
    Вивести загальну суму балів, яку отримала кожна команда
    """
    cur.execute(query_1)
    constructors = []
    total = []

    for row in cur:
        constructors.append(row[0])
        total.append(row[1])

    x_range = range(len(constructors))

    bar_ax.bar(x_range, total, label='Total')
    bar_ax.set_title('Загальна сума балів, яку отримала кожна команда')
    bar_ax.set_xlabel('Команди')
    bar_ax.set_ylabel('Сума балів')
    bar_ax.set_xticks(x_range)
    bar_ax.set_xticklabels(constructors)
    bar_ax.legend()

    """
    Другий запит
    Вивести кількість пілотів кожної національності
    """
    cur.execute(query_2)
    driver_nationality = []
    total = []

    for row in cur:
        driver_nationality.append(row[0])
        total.append(row[1])

    pie_ax.pie(total, labels=driver_nationality, autopct='%1.1f%%')
    pie_ax.set_title('Кількість пілотів кожної національності')
    #pie_ax.legend()
    # легенда до кругової діаграми

    """
    Третій запит
    Вивести графік залежності балів від фінальної позиції, на яку приїхав пілот
    """
    cur.execute(query_3)
    position = []
    points = []

    for row in cur:
        position.append(row[0])
        points.append(row[1])

    graph_ax.plot(position, points, marker='o')
    graph_ax.set_xlabel('Позиція')
    graph_ax.set_ylabel('Кількість балів')
    graph_ax.set_title('Графік залежності балів від фінальної позиції, на яку приїхав пілот')

    for qnt, price in zip(position, points):
        graph_ax.annotate(price, xy=(qnt, price), xytext=(7, 2), textcoords='offset points')

plt.tight_layout()
plt.show()