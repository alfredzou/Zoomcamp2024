import requests
import json

download_url = "https://storage.googleapis.com/dtc_zoomcamp_api/yellow_tripdata_2009-06.jsonl"


def stream_download_jsonl(download_url):
    response = requests.get(download_url, stream=True)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    for line in response.iter_lines():
        if line:
            yield json.loads(line)


if __name__ == "__main__":
    # time the download
    import time

    start = time.time()

    # Use the generator to iterate over rows with minimal memory usage
    row_counter = 0
    for row in stream_download_jsonl(download_url):
        print(row)
        row_counter += 1
        if row_counter >= 5:
            break

    # time the download
    end = time.time()
    print(end - start)
