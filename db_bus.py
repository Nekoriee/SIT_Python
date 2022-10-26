import sqlite3
import pandas as pd

con = sqlite3.connect("bus.sqlite")

f_damp = open('bus.db','r', encoding ='utf-8-sig')
damp = f_damp.read()
f_damp.close()
con.executescript(damp)
con.commit

cursor = con.cursor()

# Выборка 1
print('Выборка 1 - рейсы, которые выехали до 12:00 20-го Октября, по возрастанию времени')
df = pd.read_sql(''' 
    SELECT
            route_name as Маршрут,
            bus_number as Автобус,
            driver_name as Водитель,
            trip_timestart as Время_выезда
        FROM trip, route, bus, driver
        WHERE
            trip.bus_id = bus.bus_id
            AND trip.route_id = route.route_id
            AND trip.driver_id = driver.driver_id
            AND strftime('%s', trip_timestart) < strftime('%s', :dattim)
            AND date(trip_timestart) = date(:dattim)
        ORDER BY Время_выезда ASC
''', con, params={"dattim" : '2022-10-20 12:00:00'}) 
print(df, "\n")

# Выборка 2
print('Выборка 2 - маршруты, которые проходят через остановку Луговая, по убыванию номера')
df = pd.read_sql(''' 
    SELECT
            bustop_name as Остановка,
            route_name as Маршрут
        FROM bustop_route, route, bustop
        WHERE
            bustop_route.bustop_id = bustop.bustop_id
            AND bustop_route.route_id = route.route_id
            AND bustop_name = :stop
        ORDER BY route_name DESC

''', con, params={"stop" : 'Луговая'}) 
print(df, "\n")

# Группировка 1
print('Группировка 1 - время, затрачиваемое на весь маршрут, по возрастанию времени')
df = pd.read_sql(''' 
    SELECT
            route_name as Маршрут,
            max(bustop_route_movetime) as Время_в_пути
        FROM
            bustop_route
            JOIN route ON bustop_route.route_id = route.route_id
        GROUP BY route_name
        ORDER BY Время_в_пути ASC

''', con) 
print(df, "\n")

# Группировка 2
print('Группировка 2 - среднее время выезда для каждого маршрута 20-го Октября, по возрастанию времени')
df = pd.read_sql(''' 
    SELECT
            route_name as Маршрут,
            datetime(avg(strftime('%s', trip_timestart)), 'unixepoch') as Среднее_время_выезда
        FROM
            trip
            JOIN route ON trip.route_id = route.route_id
        WHERE
            date(trip_timestart) = :dat
        GROUP BY route_name
        ORDER BY Среднее_время_выезда ASC

''', con, params={"dat" : '2022-10-20'}) 
print(df, "\n")

# Подзапросы 1
print('Подзапросы 1 - маршруты, пролегающие через наибольшее число остановок, по убыванию номера маршрута')
df = pd.read_sql(''' 
    SELECT
            route_name as Маршрут,
            count() as Число_остановок
        FROM
            bustop_route
            JOIN route ON bustop_route.route_id = route.route_id
        GROUP BY route_name
        HAVING
            Число_остановок = (
                SELECT count() as cnt
                FROM bustop_route
                GROUP BY route_id
                ORDER BY cnt DESC
                LIMIT 1
            )
        ORDER BY route_name ASC
''', con) 
print(df, "\n")

# Подзапросы 2
print('Подзапросы 2 - водители с наибольшим числом выездов, по алфавиту')
df = pd.read_sql(''' 
    SELECT
            driver_name as Водитель,
            count() as Число_рейсов
        FROM
            trip
            JOIN driver ON trip.driver_id = driver.driver_id
        GROUP BY driver_name
        HAVING
            Число_рейсов = (
                SELECT count() as cnt
                FROM trip
                GROUP BY driver_id
                ORDER BY cnt DESC
                LIMIT 1
            )
        ORDER BY driver_name ASC
''', con) 
print(df, "\n")

# Корректировка 1
print('Корректировка 1 - изменение времени до остановок для маршрута 77')

df = pd.read_sql(''' 
    SELECT * FROM bustop_route
    WHERE route_id = (SELECT route_id
            FROM route
            WHERE route_name = :rout
    )
''', con, params={"min" : '5', "rout" : '77'}) 
print(df, "\n")

cursor.execute(''' 
    UPDATE bustop_route
    SET
        bustop_route_movetime = bustop_route_movetime + :min
    WHERE
        bustop_route_movetime <> 0
        AND route_id = (SELECT route_id
            FROM route
            WHERE route_name = :rout
        )
    
''', {"min" : '5', "rout" : '77'})

df = pd.read_sql(''' 
    SELECT * FROM bustop_route
    WHERE route_id = (SELECT route_id
            FROM route
            WHERE route_name = :rout
    )
''', con, params={"min" : '5', "rout" : '77'}) 
print(df, "\n")

# Корректировка 2
print('Корректировка 2 - подсчёт времени прибытия для каждой остановки каждого маршрута')
cursor.execute(''' 
    INSERT INTO time
    SELECT
        bustop_route_id,
        trip_id,
        datetime(trip_timestart, '+' || bustop_route_movetime || ' minutes') as time_arrivetime
    FROM
        bustop_route
        JOIN trip on bustop_route.route_id = trip.route_id
        
''')

df = pd.read_sql(''' 
    SELECT * FROM time
    ORDER BY trip_id
''', con) 
print(df, "\n")

con.close()
