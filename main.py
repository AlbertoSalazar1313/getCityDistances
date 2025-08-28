import mysql.connector
import pyexcel_ods
import os

inputFile = "D:/Alberto/Proyectos/kilometrajes.ods"
script = "D:/Alberto/Proyectos/kilometrajes.sql"

DB_CONFIG = {
    'host': '',
    'port': ,
    'user': '',
    'password': '',
    'database': '',
    'charset': ''
}

def readFile(route):
    inputaData = pyexcel_ods.get_data(route)
    docSheet = list(inputaData.values())[0]
    docHeaders = docSheet[0]
    mainCityId = docHeaders.index("Id_ciudad")
    linkedCityId = docHeaders.index("Id_ciudad_hijo")
    rows = docSheet[1:]
    return [(row[mainCityId], row[linkedCityId]) for row in rows if len(row) >= 2]

def generaScript(cursor, mainCityId, linkedCityId):
    query = f"""
    SELECT 
        IF(idorigen={mainCityId},{linkedCityId},idorigen) AS idorigen,
        IF(iddestino={mainCityId},{linkedCityId},iddestino) AS iddestino,
        kilometraje, idpersonal, "CURDATE()" AS fechamod, "CURTIME()" AS horamod, tiemporecorrido
    FROM camiones.kilometraje 
    WHERE idorigen = {mainCityId} OR iddestino = {mainCityId}
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    inserts = []
    for row in rows:
        query = f"SELECT * FROM camiones.kilometraje WHERE idorigen = {row[0]} AND iddestino = {row[1]}"
        cursor.execute(query)
        rows2 = cursor.fetchall()
        if len(rows2) == 0:
            insert = (
                f"INSERT IGNORE INTO camiones.kilometraje"
                f"(idorigen, iddestino, kilometraje, idpersonal, fechamod, horamod, tiemporecorrido) "
                f"VALUES({row[0]},{row[1]},{row[2]},{row[3]},CURDATE(),CURTIME(),{row[6]});"
            )
            inserts.append(insert)
    return inserts

def main():
    print("Establishing connection to DB...")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("Reading ODS file...")
    cities = readFile(inputFile)

    allInserts = []

    for mainCityId, linkedCityId in cities:
        print(f"Processing: city={mainCityId}, linkedCity={linkedCityId}")
        inserts = generaScript(cursor, mainCityId, linkedCityId)
        allInserts.extend(inserts)

    cursor.close()
    conn.close()

    print(f"Created inserts on {script}...")
    os.makedirs(os.path.dirname(script), exist_ok=True)
    with open(script, 'w', encoding='utf-8') as f:
        f.write("\n".join(allInserts))

    print("All Done!")

if __name__ == "__main__":
    main()
