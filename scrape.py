import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv

# URL template
template = "http://friend.com.kp/index.php/eng/media/view/{id}/1"

# CSV file to store the extracted data
csv_file = "image_data.csv"

# Function to fetch and parse a single page
def fetch_page(id):
    url = template.format(id=id)
    print(f"Trying URL: {url}")
    
    # Send HTTP GET request
    try:
        r = requests.get(url)
        
        # Check for redirection (status code 3xx)
        if r.history and r.history[0].status_code in range(300, 400):
            return id, 'redirection', None, None, None, None

        
        # Parse the content with BeautifulSoup
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # Find the div with the class "wrapper"
        div_wrapper = soup.find('div', class_='wrapper')

        # Find the div with the class "detail-td-info-top" for title, visits, and likes
        div_info_top = soup.find('div', class_='detail-td-info-top')
        
        if div_info_top:
            # Extract the title
            title = div_info_top.find('div', class_='detail-td-title').get_text(strip=True)
            
            # Extract the visit count
            visit_count = div_info_top.find('div', class_='detail-td-read-count').get_text(strip=True).replace('Visit', '')
            
            # Extract the good (like) count
            good_count = div_info_top.find('div', class_='detail-td-sel-count').get_text(strip=True).replace('Good', '')
        else:
            title = "No title"
            visit_count = "0"
            good_count = "0"

        # If the wrapper div exists, extract all nested image URLs
        if div_wrapper:
            img_urls = [img['src'] for img in div_wrapper.find_all('img')]
            return id, 'success', title, visit_count, good_count, img_urls
        else:
            return id, 'no_images', None, None, None, None

    except requests.RequestException as e:
        return id, 'error', str(e), None, None, None

# Start ID value
start_id = 1419

# Function to run requests in parallel
def fetch_in_parallel(start_id, batch_size=25):
    current_id = start_id
    all_failed = False

    # Write CSV header if the file is new
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["ID", "Title", "Visit Count", "Good Count", "Image URL"])
    
    # Continue fetching in parallel until all requests in a batch fail
    while not all_failed:
        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            # List to store futures for each batch of 25 requests
            futures = [executor.submit(fetch_page, id) for id in range(current_id, current_id + batch_size)]
            
            # Keep track of failed requests in the batch
            failed_count = 0
            
            # Process completed tasks as they finish
            for future in as_completed(futures):
                id, status, title, visit_count, good_count, img_urls = future.result()

                if status == 'redirection':
                    print(f"Redirection detected at ID: {id}.")
                    failed_count += 1
                elif status == 'success':
                    print(f"Found {len(img_urls)} images at ID {id}:")
                    # Save the results to the CSV file
                    with open(csv_file, 'a', newline='') as f:
                        writer = csv.writer(f)
                        for img_url in img_urls:
                            full_url = f"http://friend.com.kp{img_url}"  # Add base URL to the image src
                            writer.writerow([id, title, visit_count, good_count, full_url])
                            print(full_url)
                elif status == 'no_images':
                    print(f"No images found at ID {id}.")
                    failed_count += 1
                elif status == 'error':
                    print(f"Error fetching ID {id}: {title}")
                    failed_count += 1

            # Check if all 25 requests failed in the batch
            if failed_count == batch_size:
                print(f"All {batch_size} requests failed. Stopping.")
                all_failed = True
            else:
                # Move to the next batch of IDs
                current_id += batch_size

# Example: Start fetching from ID 1419 with batch size of 25
fetch_in_parallel(start_id, batch_size=50)

