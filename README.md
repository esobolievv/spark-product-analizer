# spark-product-analizer

Description


Assume we have a system to store the information about warehouses and goods amounts.


We have following data with formats:

List of warehouse positions. Each record for particular position is unique. Format: { positionId: Long, warehouse: String, product: String, eventTime: Timestamp }
List of amounts. Records for particular position can be repeated. The latest record means current amount. Format: { positionId: Long, amount: BigDecimal, eventTime: Timestamp }.

Using Apache Spark implement the following methods using Dataframe/Dataset API:

Load required data from files, e.g. csv of json.
Find the current amount for each position, warehouse, product.
Find max, min, avg amounts for each warehouse and product.


Example inputs :


# list of warehouse positions (positionId, warehouse, product, eventTime)

1, W-1, P-1, 1528463098

2, W-1, P-2, 1528463100

3, W-2, P-3, 1528463110

4, W-2, P-4, 1528463111


# list of amounts (positionId, amount, eventTime)

1, 10.00, 1528463098  # current amount

1, 10.20, 1528463008  

2, 5.00,   1528463100   # current amount

3, 4.90,   1528463100

3, 5.50,   1528463111  # current amount

3, 5.00,   1528463105  

4, 99.99, 1528463111

4, 99.57, 1528463112   # current amount





Outputs:

# current amount for each position, warehouse, product

1, W-1, P-1, 10.20

2, W-1, P-2, 5.00

……


# max, min, avg amounts for each warehouse and product

W-1, P-1, <max?>, <min?>, <avg?>

…..

W-2, P-4, <max?>, <min?>, <avg?>
