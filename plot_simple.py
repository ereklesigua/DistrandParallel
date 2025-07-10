

from cassandra.cluster import Cluster
import pandas as pd
import matplotlib.pyplot as plt


cluster = Cluster(['localhost'])
session = cluster.connect('sensor_data')


result = session.execute("SELECT timestamp, temperature, solar_irradiation FROM environmental_readings")
df = pd.DataFrame(result._current_rows)


plt.figure(figsize=(12, 6))

plt.subplot(2, 1, 1)
plt.plot(df['timestamp'], df['temperature'], marker='o', linestyle='-', color='blue')
plt.title('Temperature Over Time')
plt.xlabel('Timestamp')
plt.ylabel('Temperature (°C)')
plt.grid(True)

plt.subplot(2, 1, 2)
plt.plot(df['timestamp'], df['solar_irradiation'], marker='o', linestyle='-', color='orange')
plt.title('Solar Irradiation Over Time')
plt.xlabel('Timestamp')
plt.ylabel('Solar Irradiation (W/m²)')
plt.grid(True)

plt.tight_layout()
plt.savefig('multi_plot.png')
print("Plot saved as multi_plot.png")