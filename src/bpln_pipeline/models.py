import bauplan


# bauplan decorator for the Python function
@bauplan.python('3.11', pip={'pandas': '2.2.0'})
@bauplan.model(
  # since this is a demo, make the table statetless and replace it everytime
  # to simplify running the demo on your own - 'APPEND' instead of 'REPLACE'
  # could be used in a real-world scenario
  materialization_strategy='REPLACE',
  # this name matches the name of the output port
  name='amountStatsTable',
  # this is the schema of the output port, both as a reference
  # and for potential checks in bauplan
  columns=[
    'tripDate',
    'avgTotal',
    'avgTip',
    'avgTolls',
  ]
)
def calculate_trip_stats(
    trips=bauplan.Model(
      'tripsTable',
      # this both serves as a reference to the input port and as a way to
      # push down the column selection to object storage for maximum performance
      columns=[
        'tpep_pickup_datetime',
        'Tip_amount', 
        'Tolls_amount',
        'Total_amount'
      ]
    )
):
    """

    Given a set of rows, each representing a specific trip in a date, calculate the average of the numerical columns,
    and rename the date column to match the output port schema. The final table will be persisted in the system, 
    and will have the following schema (one row per date):
    
    | tripDate     | avgTotal | avgTip | avgTolls |
    |------------- |----------|--------|----------|
    | "2020-01-01" | 12.3     | 1.2    | 0.1      |

    """
    import pandas as pd
    print(f"\n\n===> Number of trips retrieved: {trips.num_rows}\n\n")
    df = trips.to_pandas()
    # calculate the average of the numerical columns
    result = df.groupby("date_as_string", as_index=False).agg(
        avgTip=("Tip_amount", "mean"),
        avgTolls=("Tolls_amount", "mean"),
        avgTotal=("Total_amount", "mean"),
    )
    # rename the date column to match the output port schema
    result.rename(columns={"tpep_pickup_datetime": "tripDate"}, inplace=True)
    
    # return the table to the system for persistence
    return result


