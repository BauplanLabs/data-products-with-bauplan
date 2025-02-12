import bauplan


@bauplan.python('3.10', pip={'polars': '1.19.0'})
@bauplan.model(materialization_strategy='REPLACE')
def extract_customer_id_from_logs(
    customer_logs=bauplan.Model(
      'ddog.datadog_logs',
      columns=[
        'message',
        'request_id',
      ],
      filter="event_timestamp > $start_date AND type='customer' AND function_name=$funct_name"
    )
):
    """

    Given a set of logs of type 'customer', extract the customer id from the free-form logging
    and return a table mapping the request id to the customer id, for joining with other tables
    downstream:
    
    | request_id | customer_id |
    |------------|-------------|
    | 123        | 456         |

    """
    import pyarrow as pa
    print(f"\n\n===> Number of customer logs retrieved: {customer_logs.num_rows}\n\n")
    # process the logs and extract the customer id
    # e.g. 'message': '######## 8 ########' > 8
    messages = customer_logs['message'].to_pylist()
    customer_ids = [ int(_.split(' ')[1]) for _ in messages ]
    customer_logs = customer_logs.append_column('customer_id', pa.array(customer_ids))
    # return the table with the request id and the customer id
    return customer_logs.select(['request_id', 'customer_id'])


