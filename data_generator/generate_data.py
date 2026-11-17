
import os
import random
import pandas as pd

from datetime import datetime, timedelta


# ==================================================
# 1. CONFIGURATION GENERALE
# ==================================================

random.seed(42)

OUTPUT_DIR = "data/raw"

os.makedirs(OUTPUT_DIR, exist_ok=True)

NUMBER_CUSTOMERS = 500
NUMBER_PRODUCTS = 100
NUMBER_NORMAL_TRANSACTIONS = 9800

START_DATE = datetime.now() - timedelta(days=30)


# ==================================================
# 2. GENERATION DES CLIENTS
# ==================================================

cities = [
    "Fes",
    "Meknes",
    "Casablanca",
    "Rabat",
    "Marrakech",
    "Tanger",
    "Agadir",
    "Oujda",
    "Kenitra",
    "Tetouan"
]

first_names = [
    "Youssef", "Mohamed", "Sara", "Salma",
    "Amine", "Omar", "Nour", "Imane",
    "Hamza", "Khadija", "Adam", "Aya",
    "Mehdi", "Meryem", "Anas"
]

last_names = [
    "Alaoui", "Bennani", "Amrani", "Idrissi",
    "El Fassi", "Tazi", "Berrada", "Mansouri",
    "Chraibi", "Naciri", "Saidi", "Fikri"
]

customers = []

for i in range(1, NUMBER_CUSTOMERS + 1):

    customer = {
        "customer_id": f"CUST{i:04d}",
        "customer_name": (
            random.choice(first_names)
            + " "
            + random.choice(last_names)
        ),
        "city": random.choice(cities)
    }

    customers.append(customer)


customers_df = pd.DataFrame(customers)

customers_df.to_csv(
    f"{OUTPUT_DIR}/customers.csv",
    index=False,
    encoding="utf-8"
)

print(f"{len(customers_df)} clients generes.")


# ==================================================
# 3. GENERATION DES PRODUITS
# ==================================================

product_categories = {
    "Electronics": (500, 8000),
    "Computers": (3000, 15000),
    "Clothing": (50, 800),
    "Food": (10, 300),
    "Home": (100, 3000),
    "Sports": (100, 2500),
    "Books": (30, 300)
}

products = []

for i in range(1, NUMBER_PRODUCTS + 1):

    category = random.choice(
        list(product_categories.keys())
    )

    min_price, max_price = product_categories[category]

    reference_price = round(
        random.uniform(min_price, max_price),
        2
    )

    product = {
        "product_id": f"PROD{i:04d}",
        "product_name": f"{category}_Product_{i}",
        "category": category,
        "reference_price": reference_price
    }

    products.append(product)


products_df = pd.DataFrame(products)

products_df.to_csv(
    f"{OUTPUT_DIR}/products.csv",
    index=False,
    encoding="utf-8"
)

print(f"{len(products_df)} produits generes.")


# ==================================================
# 4. PREPARATION DES DONNEES
# ==================================================

payment_methods = [
    "Cash",
    "Card",
    "Mobile Payment",
    "Bank Transfer"
]

customer_ids = customers_df["customer_id"].tolist()

product_records = products_df.to_dict("records")

transactions = []


def create_transaction(
    customer_id,
    product,
    timestamp,
    quantity,
    unit_price,
    anomaly_type="none"
):

    return {
        "transaction_id": (
            f"TRX{len(transactions) + 1:07d}"
        ),
        "timestamp": timestamp.isoformat(),
        "customer_id": customer_id,
        "product_id": product["product_id"],
        "category": product["category"],
        "quantity": quantity,
        "unit_price": round(unit_price, 2),
        "reference_price": product["reference_price"],
        "total_amount": round(quantity * unit_price, 2),
        "payment_method": random.choice(payment_methods),
        "city": random.choice(cities),
        "is_anomaly": 1 if anomaly_type != "none" else 0,
        "anomaly_type": anomaly_type
    }


# ==================================================
# 5. TRANSACTIONS NORMALES
# ==================================================

for i in range(NUMBER_NORMAL_TRANSACTIONS):

    customer_id = random.choice(customer_ids)

    product = random.choice(product_records)

    quantity = random.randint(1, 5)

    unit_price = product["reference_price"]

    timestamp = START_DATE + timedelta(
        seconds=random.randint(0, 30 * 24 * 60 * 60)
    )

    transaction = create_transaction(
        customer_id,
        product,
        timestamp,
        quantity,
        unit_price
    )

    transactions.append(transaction)


print("Transactions normales generees.")


# ==================================================
# 6. ANOMALIE 1 : FREQUENCE EXCESSIVE
# ==================================================

# Un client effectue 100 achats en moins de 3 minutes.

frequent_customer = customer_ids[0]

burst_start = START_DATE + timedelta(days=20)

for i in range(100):

    product = random.choice(product_records)

    timestamp = burst_start + timedelta(
        seconds=random.randint(0, 179)
    )

    transaction = create_transaction(
        frequent_customer,
        product,
        timestamp,
        quantity=1,
        unit_price=product["reference_price"],
        anomaly_type="high_frequency"
    )

    transactions.append(transaction)


print("100 transactions de frequence excessive generees.")


# ==================================================
# 7. ANOMALIE 2 : MONTANT ANORMALEMENT ELEVE
# ==================================================

# Creer 50 transactions dont le montant depasse 50 000.

expensive_products = [
    product
    for product in product_records
    if product["reference_price"] >= 1500
]

for i in range(50):

    customer_id = random.choice(customer_ids)

    product = random.choice(expensive_products)

    quantity = 40

    timestamp = START_DATE + timedelta(
        seconds=random.randint(0, 30 * 24 * 60 * 60)
    )

    transaction = create_transaction(
        customer_id,
        product,
        timestamp,
        quantity=quantity,
        unit_price=product["reference_price"],
        anomaly_type="high_amount"
    )

    transactions.append(transaction)


print("50 transactions de montant eleve generees.")


# ==================================================
# 8. ANOMALIE 3 : PRIX UNITAIRE SUSPECT
# ==================================================

# Le prix applique est 5 fois le prix de reference.

for i in range(50):

    customer_id = random.choice(customer_ids)

    product = random.choice(product_records)

    suspicious_price = product["reference_price"] * 5

    timestamp = START_DATE + timedelta(
        seconds=random.randint(0, 30 * 24 * 60 * 60)
    )

    transaction = create_transaction(
        customer_id,
        product,
        timestamp,
        quantity=1,
        unit_price=suspicious_price,
        anomaly_type="suspicious_price"
    )

    transactions.append(transaction)


print("50 transactions avec prix suspects generees.")


# ==================================================
# 9. CREATION DU CSV FINAL
# ==================================================

transactions_df = pd.DataFrame(transactions)

# Trier les transactions par date
transactions_df["timestamp"] = pd.to_datetime(
    transactions_df["timestamp"]
)

transactions_df = transactions_df.sort_values(
    by="timestamp"
)

# Sauvegarder les transactions
transactions_df.to_csv(
    f"{OUTPUT_DIR}/transactions.csv",
    index=False,
    encoding="utf-8"
)


# ==================================================
# 10. STATISTIQUES FINALES
# ==================================================

print("\n===================================")
print("GENERATION TERMINEE")
print("===================================")

print(f"Clients : {len(customers_df)}")
print(f"Produits : {len(products_df)}")
print(f"Transactions : {len(transactions_df)}")

print("\nRepartition des transactions :")

print(
    transactions_df["anomaly_type"].value_counts()
)

print("\nNombre total d'anomalies :")

print(
    transactions_df["is_anomaly"].sum()
)

print("\nFichiers generes dans :", OUTPUT_DIR)
