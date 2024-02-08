import requests


api_url = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"

# I call this a paginated getter
# as it's a function that gets data
# and also paginates until there is no more data
# by yielding pages, we "microbatch", which speeds up downstream processing


def paginated_getter(api_url):
    page_number = 1

    while True:
        # Set the query parameters
        params = {"page": page_number}

        # Make the GET request to the API
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        page_json = response.json()
        print(f"got page number {page_number} with {len(page_json)} records")

        # if the page has no records, stop iterating
        if page_json:
            yield page_json
            page_number += 1
        else:
            # No more data, break the loop
            break


if __name__ == "__main__":
    # Use the generator to iterate over pages
    for page_data in paginated_getter(api_url):
        # Process each page as needed
        print(page_data)
