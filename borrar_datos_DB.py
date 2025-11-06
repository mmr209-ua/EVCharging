import sqlite3

BBDD = "Base_Datos.sqlite"

with sqlite3.connect(BBDD) as conn:
    cur = conn.cursor()
    cur.execute("DELETE FROM CP")
    conn.commit()
    cur.execute("DELETE FROM CONSUMO")
    conn.commit()

print("Borrar datos insertados en la tabla CP.")
print("Borrar datos insertados en la tabla CONSUMO.")