import dlt
import duckdb

data = [
    {
        "vendor_name": "VTS",
				"record_hash": "b00361a396177a9cb410ff61f20015ad",
        "time": {
            "pickup": "2009-06-14 23:23:00",
            "dropoff": "2009-06-14 23:48:00"
        },
        "Trip_Distance": 17.52,
        # nested dictionaries could be flattened
        "coordinates": { # coordinates__start__lon
            "start": {
                "lon": -73.787442,
                "lat": 40.641525
            },
            "end": {
                "lon": -73.980072,
                "lat": 40.742963
            }
        },
        "Rate_Code": None,
        "store_and_forward": None,
        "Payment": {
            "type": "Credit",
            "amt": 20.5,
            "surcharge": 0,
            "mta_tax": None,
            "tip": 9,
            "tolls": 4.15,
						"status": "booked"
        },
        "Passenger_Count": 2,
        # nested lists need to be expressed as separate tables
        "passengers": [
            {"name": "John", "rating": 4.9},
            {"name": "Jack", "rating": 3.9}
        ],
        "Stops": [
            {"lon": -73.6, "lat": 40.6},
            {"lon": -73.5, "lat": 40.5}
        ]
    },
    # ... more data
]


# define the connection to load to.
# We now use duckdb, but you can switch to Bigquery later
pipeline = dlt.pipeline(pipeline_name = "taxi_data", destination='duckdb', dataset_name='taxi_rides')



# run with merge write disposition.
# This is so scaffolding is created for the next example,
# where we look at merging data

info = pipeline.run(data,
										table_name="rides",
										write_disposition="merge",
                    primary_key="record_hash")

print(info)

conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
conn.sql("show tables")

query = """
SELECT * FROM rides
LEFT JOIN rides__passengers pax
    ON rides._dlt_id = pax._dlt_parent_id
LEFT JOIN rides__stops stops
    ON rides._dlt_id = stops._dlt_parent_id
"""

conn.sql(query)
