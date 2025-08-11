import time, requests, pandas as pd

def fetch_api(url, params=None, headers=None, retries=3, sleep=2):
    for i in range(retries):
        r = requests.get(url, params=params, headers=headers, timeout=30)
        if r.ok:
            data = r.json()
            return pd.json_normalize(data)
        time.sleep(sleep)
    raise RuntimeError(f"API failed after {retries} retries: {url}")