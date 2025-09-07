import os
import pandas as pd
import psycopg2
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report

# --- Database Connection ---
def get_db_connection():
    return psycopg2.connect(
        dbname=os.environ.get("POSTGRES_DB","sqldb"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "garvit005"),
        host="127.0.0.1", # When running locally, connect to localhost
        port="5432"
    )

# --- Generate Fake Data (For Initial Training) ---
def generate_fake_data(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM optimization_logs;")
    if cursor.fetchone()[0] > 0:
        print("Data already exists. Skipping data generation.")
        return

    print("Generating expanded fake training data...")
    # We need enough data for the smallest class to appear in the test split.
    fake_data = [
        # (join_count, where_count, improvement) -> class
        (1, 1, 5.5),   # Low
        (1, 0, 8.1),   # Low
        (0, 1, 7.3),   # Low
        (1, 1, 4.2),   # Low
        (1, 2, 15.8),  # Medium
        (0, 2, 25.6),  # Medium
        (2, 1, 19.1),  # Medium
        (2, 2, 22.0),  # Medium
        (1, 2, 45.2),  # High
        (2, 2, 65.1),  # High
        (3, 3, 75.9),  # High
        (0, 1, 35.0),  # High
        (2, 3, 55.3),  # High
        (3, 1, 68.0),  # High
        (1, 3, 51.0),  # High
    ]
    for join_count, where_count, improvement in fake_data:
        cursor.execute(
            "INSERT INTO optimization_logs (join_count, where_clause_count, performance_improvement_percent) VALUES (%s, %s, %s)",
            (join_count, where_count, improvement)
        )
    conn.commit()
    print("Fake data inserted.")

# --- Main Training Logic ---
def train_model():
    conn = get_db_connection()
    
    # Ensure we have some data to train on
    generate_fake_data(conn)
    
    # 1. Load data into a DataFrame
    df = pd.read_sql("SELECT join_count, where_clause_count, performance_improvement_percent FROM optimization_logs", conn)
    conn.close()

    if df.empty:
        print("No data found to train the model.")
        return

    # 2. Feature Engineering
    # Define our features (X)
    features = ['join_count', 'where_clause_count']
    X = df[features]

    # Create our target variable (y) by binning the percentage into classes
    bins = [-1, 10, 30, 101] # 0-10% = Low, 10-30% = Medium, >30% = High
    labels = ['Low', 'Medium', 'High']
    y = pd.cut(df['performance_improvement_percent'], bins=bins, labels=labels)

    # 3. Train/Test Split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 4. Model Training
    print("\nTraining RandomForestClassifier...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # 5. Evaluate Model
    print("\nModel Evaluation:")
    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred, zero_division=0))

    # 6. Save the trained model
    model_path = os.path.join(os.path.dirname(__file__), 'model.joblib')
    joblib.dump(model, model_path)
    print(f"✅ Model saved successfully to {model_path}")


if __name__ == '__main__':
    train_model()