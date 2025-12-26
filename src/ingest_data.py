import requests
import pandas as pd
import os

def ingest_users():
    # 1. Define URL (Nguồn dữ liệu)
    api_url = "https://jsonplaceholder.typicode.com/users"
    
    print(f"Fetching data from {api_url}...")
    
    # 2. Call API (Lấy dữ liệu)
    try:
        response = requests.get(api_url)
        response.raise_for_status() # Báo lỗi nếu status code không phải 200 (OK)
        data = response.json()
        print(f"Successfully fetched {len(data)} records.")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return

    # 3. Transform (Chuyển đổi dữ liệu)
    df = pd.read_json(api_url) # Pandas có thể đọc thẳng từ URL hoặc dùng biến 'data' ở trên
    
    # Chỉ lấy các cột cần thiết (Column selection)
    selected_columns = ['id', 'name', 'username', 'email', 'website']
    df_clean = df[selected_columns]
    
    # 4. Load (Lưu trữ)
    output_dir = "data/raw"
    output_file = "users.csv"
    output_path = os.path.join(output_dir, output_file)
    
    # Đảm bảo thư mục tồn tại trước khi lưu
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Saving data to {output_path}...")
    df_clean.to_csv(output_path, index=False)
    print("Data ingestion complete!")

if __name__ == "__main__":
    ingest_users()