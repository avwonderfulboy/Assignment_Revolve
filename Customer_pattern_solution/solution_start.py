import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="../input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="../input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="../input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="../output_data/outputs/")
    return vars(parser.parse_args())

def preprocess_data(spark, customers_location, products_location, transactions_location, output_location):
    # Load the customer data from CSV
    customer_df = spark.read.csv(customers_location, header=True)

    # Load the transaction data from JSON Lines
    transaction_df = spark.read.json(transactions_location)

    # Load the product data from CSV
    product_df = spark.read.csv(products_location, header=True)

    


# Requirement - customer_id, loyalty_score, product_id, product_category, purchase_count

    transaction_data= (transaction_df
                       .withColumn("product_id",explode('basket.product_id'))
                       .select('product_id','customer_id')
        )

    #joining customer data , transcation data and product
    #selecting the requiered column

    joinedData= (transaction_data
                 .join(customer_df,transaction_data.customer_id ==  customer_df.customer_id) 
                 .join(product_df,transaction_data.product_id == product_df.product_id)
                 .select(customer_df.customer_id,customer_df.loyalty_score,product_df.product_id,product_df.product_category)
                 
     )
    
    # getting the  purchase count   
    result = joinedData.groupBy("customer_id", "product_id", "product_category").count().withColumnRenamed("count", "purchase_count")
    
    # Writing the JSON File
    result.write.json(output_location)
    print(output_location)

# customer_df-Schema    
# root                                                                            
#  |-- customer_id: string (nullable = true)
#  |-- loyalty_score: string (nullable = true)
#transaction_df -Schema
#   root                                                                            
#  |-- basket: array (nullable = true)
#  |    |-- element: struct (containsNull = true)
#  |    |    |-- price: long (nullable = true)
#  |    |    |-- product_id: string (nullable = true)
#  |-- customer_id: string (nullable = true)
#  |-- date_of_purchase: string (nullable = true)
#  |-- d: date (nullable = true)

# Product_df-Schema
# root                                                                            
#  |-- product_id: string (nullable = true)
#  |-- product_description: string (nullable = true)
#  |-- product_category: string (nullable = true)

def main():
    params = get_params()

    # Create a Spark session
    spark = SparkSession.builder.appName("DataPreprocessing").getOrCreate()

    # Call the preprocess_data function with the provided arguments
    preprocess_data(
        spark,
        params['customers_location'],
        params['products_location'],
        params['transactions_location'],
        params['output_location']
    )

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()
