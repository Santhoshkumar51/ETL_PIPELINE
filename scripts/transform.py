# ===========================
# transform.py
# ===========================
 
import os
import pandas as pd
 
# Purpose: Clean and transform Customer dataset
def transform_data(raw_path):
    # Ensure the path is relative to project root
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # go up one level
    staged_dir = os.path.join(base_dir, "data", "staged")
    os.makedirs(staged_dir, exist_ok=True)
 
    df = pd.read_csv(raw_path)

    
    # --- 1️⃣ Transformation and Handle missing values ---
    df['TotalCharges']=pd.to_numeric(df['TotalCharges'],errors='coerce')
    df.fillna({'TotalCharges':df['TotalCharges'].mean()},inplace=True)
 
    # --- 2️⃣ Feature engineering ---
    bins=[0,13,37,61,float('inf')]
    labels=["New", "Regular","Loyal","Champion"]
    df['tenure_group']=pd.cut(df['tenure'],bins=bins,labels=labels,right=False)
    # df.dropna(subset=['tenure_group'],inplace=True)
 
    bins=[0,30,70,float('inf')]
    labels=["Low","Medium","High"]
    df['monthly_charge_segment']=pd.cut(df['MonthlyCharges'],bins=bins,labels=labels,right=True)

    df['has_internet_service']=df['InternetService'].map({ "DSL": 1,
    "Fiber optic": 1,
    "No": 0})

    df['is_multi_line_user']=df['MultipleLines'].map(
        {
            'Yes':1,
            'No':0,
            'No phone service':0
        }
    )
    
    df['contract_type_code']=df['Contract'].map(
        {
            'Month-to-month':0,
            'One year':1,
            'Two year':2
        }
    )
    
    # --- 3️⃣ Drop unnecessary columns ---
    df.drop(columns=['customerID','gender'],axis=1,inplace=True)
    df.columns=df.columns.str.lower()
 
    # --- 4️⃣ Save transformed data ---
    staged_path = os.path.join(staged_dir, "Customer_transformed.csv")
    df.to_csv(staged_path, index=False)
    print(f"✅ Data transformed and saved at: {staged_path}")
    return staged_path
 
 
if __name__ == "__main__":
    from extract import extract_data
    raw_path = extract_data()
    transform_data(raw_path)