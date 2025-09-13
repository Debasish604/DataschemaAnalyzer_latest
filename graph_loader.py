import pandas as pd
import json
from neo4j import GraphDatabase

# -------------------
# Neo4j connection
# -------------------
URI = "neo4j+s://7c650552.databases.neo4j.io"   
USER = "neo4j"
PASSWORD = "30cvHfcgWP9NvyWjOGF4ynTZEPip73uSJ-QEzTxUm9U"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# -------------------
# Cypher Helpers
# -------------------
def create_customer(tx, customer):
    query = """
    MERGE (c:Customer {CustomerID: $CustomerID})
    SET c.Name = $Name, c.Email = $Email, c.City = $City
    """
    tx.run(query, **customer)

def create_order(tx, order):
    query = """
    MERGE (o:Order {OrderID: $OrderID})
    SET o.Product = $Product, o.Quantity = $Quantity, o.OrderDate = $OrderDate
    """
    tx.run(query, **order)

def create_relationship(tx, customer_id, order_id):
    query = """
    MATCH (c:Customer {CustomerID: $cust_id})
    MATCH (o:Order {OrderID: $order_id})
    MERGE (c)-[:PLACED]->(o)
    """
    tx.run(query, cust_id=customer_id, order_id=order_id)

# -------------------
# File Loaders
# -------------------
def load_xlsx(path):
    return pd.read_excel(path)

# -------------------
# Main Logic
# -------------------
def process_and_push(customer_file, order_file):
    df_customers = load_xlsx(customer_file)
    df_orders = load_xlsx(order_file)

    with driver.session() as session:
        # Customers
        for _, row in df_customers.iterrows():
            customer = {
                "CustomerID": str(row["CustomerID"]),
                "Name": str(row["Name"]),
                "Email": str(row["Email"]),
                "City": str(row["City"])
            }
            session.execute_write(create_customer, customer)

    # Orders + relationships
    for _, row in df_orders.iterrows():
        order = {
            "OrderID": str(row["OrderID"]),
            "Product": str(row["Product"]),
            "Quantity": int(row["Quantity"]),
            "OrderDate": str(row["OrderDate"])
        }
        session.execute_write(create_order, order)
        session.execute_write(create_relationship, str(row["CustomerID"]), str(row["OrderID"]))


if __name__ == "__main__":
    customer_file = "uploads/9_Customers.xlsx"
    order_file = "uploads/9_Orders.xlsx"
    process_and_push(customer_file, order_file)
    print("✅ Customers, Orders, and Relationships loaded into Neo4j")