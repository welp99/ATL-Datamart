import psycopg2

def execute_sql_from_file(filename, connection):
    """ Exécute les commandes SQL contenues dans un fichier, une par une. """
    with open(filename, 'r') as file:
        sql_script = file.read()

    commands = sql_script.split(';')  # Sépare les commandes par des points-virgules

    for command in commands:
        if command.strip():
            with connection.cursor() as cursor:
                cursor.execute(command)
                connection.commit()

def main():
    # Paramètres de connexion pour nyc_warehouse
    warehouse_params = {
        'dbname': 'nyc_warehouse',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': '15432'
    }

    # Paramètres de connexion pour nyc_datamart
    datamart_params = {
        'dbname': 'nyc_datamart',
        'user': 'postgres',
        'password': 'admin',
        'host': 'localhost',
        'port': '15432'
    }

    conn_warehouse = None
    conn_datamart = None

    try:
        # Connexion à nyc_warehouse
        conn_warehouse = psycopg2.connect(**warehouse_params)

        # Connexion à nyc_datamart
        conn_datamart = psycopg2.connect(**datamart_params)

        # Exécution du script de création sur nyc_datamart
        execute_sql_from_file('creation.sql', conn_datamart)

        # Exécution du script d'insertion (en assumant que le script fait référence aux deux bases de données)
        execute_sql_from_file('insertion.sql', conn_datamart)

        print("Les scripts SQL ont été exécutés avec succès.")
    except psycopg2.DatabaseError as e:
        print(f"Erreur lors de l'exécution des scripts SQL: {e}")
    finally:
        if conn_warehouse is not None:
            conn_warehouse.close()
        if conn_datamart is not None:
            conn_datamart.close()

if __name__ == "__main__":
    main()
