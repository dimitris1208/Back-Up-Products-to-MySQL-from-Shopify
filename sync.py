import os
from dotenv import load_dotenv
import requests
import time

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




def main():
    start_time = time.time()

    data = fetch_all_products()
    if not data:
        print("No products found or error fetching products.")
        return