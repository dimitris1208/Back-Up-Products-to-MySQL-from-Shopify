import os
import json
import mysql.connector
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv
import requests
import time
import re


load_dotenv()


#--- Environment Variables ---

SHOP_DOMAIN = os.getenv("SHOP_DOMAIN")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
API_VERSION = os.getenv("API_VERSION", "2024-01")

PA_SSH_HOST = os.getenv("PA_SSH_HOST")
PA_SSH_USER = os.getenv("PA_SSH_USER")
PA_SSH_PASS = os.getenv("PA_SSH_PASS")
PA_DB_HOST = os.getenv("PA_DB_HOST")
PA_DB_NAME = os.getenv("PA_DB_NAME")
PA_DB_PASS = os.getenv("PA_DB_PASS")



def get_next_link(headers):
    if "Link" not in headers: return None
    match = re.search(r'<([^>]+)>;\s*rel="next"', headers["Link"])
    return match.group(1) if match else None


def fetch_all_products():
    print("Fetching all products from the e-commerce platform...")
    session = requests.Session()
    session.headers.update({
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    })

    products = []
    url = f"https://{SHOP_DOMAIN}/admin/api/{API_VERSION}/products.json?limit=250"

    while url: 
        try:
            response = session.get(url)
            if response.status_code == 429:
                time.sleep(5)  # Rate limit hit, wait before retrying
                continue
            if response.status_code != 200:
                print(f"Error fetching products: {response.status_code} - {response.text}")
                break
            data = response.json()
            products.extend(data.get("products", []))
            print(f"Fetched {len(products)} products so far...")

            url = get_next_link(response.headers)
            if url:
                time.sleep(0.5) # Brief pause to respect rate limits

        except Exception as e:
            print(f"Exception occurred: {e}")
            break
    
    return products



def transform_to_tuples(raw_products):
    rows = []
    today = time.strftime("%Y-%m-%d")
    
    for p in raw_products:
        variants = p.get('variants', [])
        total_inv = sum(v.get('inventory_quantity', 0) for v in variants)
        # Handle cases where product has no variants
        price = variants[0].get('price') if variants else 0.00
        compare = variants[0].get('compare_at_price') if variants else None
        
        image_urls = json.dumps([img['src'] for img in p.get('images', [])])
        tags = p.get('tags', '')
        if isinstance(tags, list): tags = ", ".join(tags)
        
        row = (
            p['id'],               
            today,                 
            p.get('title'),        
            p.get('body_html'),    
            p.get('vendor'),       
            p.get('product_type'),  
            p.get('handle'),        
            tags,                 
            image_urls,          
            price,                  
            compare,            
            total_inv,              
            p.get('created_at')     
        )
        rows.append(row)
        
    return rows


def main():
    start_time = time.time()

    # Fetch all products
    data = fetch_all_products()
    if not data:
        print("No products found or error fetching products.")
        return
    
    # Transform data as needed 
    print("Transforming data to Tuples...")
    rows_to_insert = transform_to_tuples(data)


    # UPLOAD
    print ("\n Opening Secure Tunnel...")
    with SSHTunnelForwarder(
        (PA_SSH_HOST, 22),
        ssh_username=PA_SSH_USER,
        ssh_password=PA_SSH_PASS,
        remote_bind_address=(PA_DB_HOST, 3306)
    ) as tunnel:    
        
        print("Connecting to Daabase...")

        conn = mysql.connector.connect(
            user=PA_SSH_USER,
            password=PA_DB_PASS,
            host='127.0.0.1',
            port=tunnel.local_bind_port,
            database=PA_DB_NAME
        )
        cursor = conn.cursor()

        today = time.strftime('%Y-%m-%d')

        #Delete todays data Idempotency
        print("Deleting existing data for today...")
        cursor.execute("DELETE FROM products WHERE snapshot_date = %s", (today,))

        # Insert new data
        print(f"Inserting {len(rows_to_insert)} new records...")

        sql = """
            INSERT INTO products (
                id, snapshot_date, title, body_html, vendor, product_type, handle, tags,
                images, price, compare_at_price, inventory_quantity, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        #Batch logic
        batch_size = 500
        for i in range(0, len(rows_to_insert), batch_size):
            batch = rows_to_insert[i:i+batch_size]
            cursor.executemany(sql, batch)
            print(f"    Saved rows {i} to {i+len(batch)}")


        conn.commit()
        cursor.close()
        conn.close()
    
    print(f"\nSync completed in {time.time() - start_time:.2f} seconds.")


        

if __name__ == "__main__":
    main()

