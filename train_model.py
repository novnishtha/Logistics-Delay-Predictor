import pandas as pd
import joblib
from sklearn.ensemble import RandomForestRegressor
from data_generator import generate_logistics_event

# Generate 5000 records to test the logic -- better model stability
data_list = [generate_logistics_event() for _ in range(5000)]
df_simulated = pd.DataFrame(data_list)

# FEATURE ENGINEERING
# remove unnecessary columns before training
features_df = df_simulated.drop(
    ['delay_minutes'], axis=1
)

X = pd.get_dummies(features_df) # encoded
y = df_simulated['delay_minutes']

# Save column order
model_columns = X.columns.tolist()

# 3. Train
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X, y)

# 4. Save
joblib.dump(model, 'rf_model.pkl')
joblib.dump(model_columns, 'model_columns.pkl')

print(f"Saved model trained on {len(df_simulated)} simulated rows.")
print(f"Features learned: {X.columns.tolist()}")