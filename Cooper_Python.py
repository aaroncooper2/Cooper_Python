import json
import requests
import csv
import os
import datetime
import threading

# defines json object from file
def clean_json(filename):
    with open(filename,'r') as f:
        data = json.load(f)
        return data

# converts field names to snake_case
def convert_to_snake_case(name):
    """
    Converts a string to snake_case.

    Args:
        name: The string to convert.

    Returns:
        The string in snake_case format.
    """
    return '_'.join(word.lower() for word in name.split()).strip('_')

"""
checks for modified_date is greater than previous run date from metadata
calls those identifiers' URLs, downloads, csv, updates column names, saves with timestamp in
filename in format - "identifier/identifier_timestamp.csv", and updates metadata file
"""
def download_and_save_data_item(item, metadata_dict, metadata_csv, lock):
    landing_page = item.get('landingPage')
    landing_page_code = landing_page.split('/')[-1]
    download_url = None

    # Extract download URL from the first distribution
    distribution = item.get('distribution', [])
    download_url = distribution[0].get('downloadURL')

    # Create timestamp for directory name
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Create directory for the dataset (landingPageCode_timestamp)
    dataset_dir = f"data/{landing_page_code}"

    # Acquire lock before checking metadata
    lock.acquire()
    last_modified_date = metadata_dict.get(landing_page_code, {}).get('last_modified_date')
    last_downloaded_date = metadata_dict.get(landing_page_code, {}).get('last_downloaded_date')
    lock.release()

    # Download only if modified date is newer
    if not last_modified_date or (item.get('modified') > last_modified_date):
        os.makedirs(dataset_dir, exist_ok=True)
        try:
            response = requests.get(download_url)
            response.raise_for_status()  # Raise exception for non-2xx status codes

            # Create CSV filename within the dataset directory
            filename = f"{dataset_dir}/{landing_page_code}_{timestamp}.csv"

            with open(filename, 'w', newline='') as csvfile:
                reader = csv.reader(response.content.decode('utf-8').splitlines()) 
                csv_writer = csv.writer(csvfile)

                # Get and convert the header row
                header = next(reader)  # Get the first row (header)
                snake_case_header = [convert_to_snake_case(col) for col in header] #convert each column to snakecase
                csv_writer.writerow(snake_case_header) 

                # Write remaining rows
                csv_writer.writerows(reader)

            print(f"Downloaded and saved data to {filename}")

            # Acquire lock before updating metadata
            lock.acquire()
            metadata_dict[landing_page_code] = {
                'landingPageCode': landing_page_code,
                'last_modified_date': item.get('modified'),
                'last_downloaded_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            writer.writerow(metadata_dict[landing_page_code]) 
            lock.release()

        except requests.exceptions.RequestException as e:
            print(f"Error downloading data from {download_url}: {e}")
        except Exception as e:  # Catch other potential exceptions (e.g., decoding errors)
            print(f"An error occurred while processing {filename}: {e}")

    else:
        print("No new data for code: ", landing_page_code)

if __name__ == "__main__":
    filename = '/Users/aaroncooper/Documents/Python_Projects/Data Files/items.json'
    json_data = clean_json(filename)

    relevant_data = []
    for element in json_data:
        if "Hospitals" in element["theme"]:
            relevant_data.append(element)

    print(len(relevant_data))
    # relevant_data = relevant_data[:5]  # Uncomment for testing with a smaller subset

    # Threading setup
    threads = []
    lock = threading.Lock()  # Create a lock for thread safety
    os.makedirs("data", exist_ok=True) 

    # Read existing metadata (if any)
    metadata_dict = {}
    if os.path.exists("data/download_metadata.csv"):
        with open("data/download_metadata.csv", 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                metadata_dict[row['landingPageCode']] = row 
    else:
        # Create metadata file and write header if it doesn't exist
        with open("data/download_metadata.csv", 'w', newline='') as metadata_csv: 
            writer = csv.DictWriter(metadata_csv, fieldnames=['landingPageCode', 'last_modified_date', 'last_downloaded_date']) 
            writer.writeheader()

    with open("data/download_metadata.csv", 'a+', newline='') as metadata_csv: 
        writer = csv.DictWriter(metadata_csv, fieldnames=['landingPageCode', 'last_modified_date', 'last_downloaded_date']) 

        for item in relevant_data:
            thread = threading.Thread(target=download_and_save_data_item, args=(item, metadata_dict, writer, lock))
            threads.append(thread)
            thread.start()

        # Wait for all threads to finish
        for thread in threads:
            thread.join()

    print("All downloads completed.")
