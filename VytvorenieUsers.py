import random
import csv

# Definovanie počtu používateľov
num_users = 610

# Otvorenie nového súboru CSV
with open('user.csv', 'w', newline='') as file:
    writer = csv.writer(file)

    # Zápis hlavičiek
    writer.writerow(['id', 'age'])

    # Zápis používateľských údajov
    for user_id in range(1, num_users + 1):
        age = random.randint(15, 70)
        writer.writerow([user_id, age])

print("user.csv has been created.")
