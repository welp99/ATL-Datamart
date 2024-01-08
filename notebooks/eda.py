import pandas as pd
import sqlalchemy
import matplotlib.pyplot as plt
import seaborn as sns

# Connexion à la base de données
engine = sqlalchemy.create_engine('postgresql://postgre:admin@192.168.1.23:15432/nyc_warehouse')
conn = engine.connect()

# Requête et chargement des données dans un DataFrame
query = "SELECT * FROM taxi_trips"  
df = pd.read_sql(query, conn)
conn.close()

# EDA 
print(df.describe())
print(df.info())

# Visualisation 1
plt.figure(figsize=(10, 6))
sns.countplot(data=df, x='passenger_count')
plt.title('Distribution du Nombre de Passagers')
plt.show()


