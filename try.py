import psycopg2
import uuid
import random

def init_users():
    conn = psycopg2.connect(
        dbname='bank_fraud',
        user='darklord',
        password='04112005',
        host='localhost',
        port=5432
    )
    cursor = conn.cursor()

    total_users = {
        "active": 500,
        "moderate": 1500,
        "low": 2000
    }

    insert_query = """
        INSERT INTO consumers (
            user_id, last_transaction_time, balance, user_profile,
            zip_history, ip_history, device_fingerprint,
            avg_txn_amount, account_age_days
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for profile, count in total_users.items():
        for _ in range(count):
            data = (
                str(uuid.uuid4()),     # user_id
                None,                  # last_transaction_time
                round(random.uniform(1000, 50000), 2),  # balance
                profile,               # user_profile
                [],                    # zip_history
                [],                    # ip_history
                None,                  # device_fingerprint
                None,                  # avg_txn_amount
                random.randint(30, 365 * 5)  # account_age_days
            )
            cursor.execute(insert_query, data)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Inserted 500 active, 1500 moderate, 2000 low users into 'consumers'.")

if __name__ == "__main__":
    init_users()
