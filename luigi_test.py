import luigi
import requests
import json
import time
import os

class StockTwitScraped(luigi.Task):

    def output(self):
        return luigi.LocalTarget("raw_data.json")

    def run(self):
        url = "https://api.stocktwits.com/api/2/streams/symbol/wy.json"
        
        # Fetch the content from the API
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            content = response.text
            try:
                # Try to decode the JSON content
                json_data = json.loads(content)
                
                # Generate a unique temporary file name
                temp_filename = f"raw_data_{int(time.time())}.json"
                
                # Write the JSON data to a temporary file
                with open(temp_filename, 'w') as f:
                    json.dump(json_data, f, indent=4)
                
                # Rename the temporary file to the final output name
                os.rename(temp_filename, 'raw_data.json')
            
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON response: {e}")
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")

