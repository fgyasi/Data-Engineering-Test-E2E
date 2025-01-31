from pyspark.sql import SparkSession
import json
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, FloatType
from pyspark.sql import functions as F

from pyspark.sql.functions import col, expr, explode, split, trim
# Initialize Spark Session
spark = SparkSession.builder.appName("DockerPySparkExample").getOrCreate()







def data_validation(df):
    df = df.withColumn("contact_data", F.when(F.isnull("contact_data"), "[]").otherwise(F.col("contact_data")))
    df = df.withColumn("date", F.to_date("date", "dd.MM.yy"))
    # Define a UDF (User Defined Function) to handle string modification
    def ensure_brackets(value):
        # Check if the string starts or ends with brackets
        value=value.strip('"')
        if not value.startswith('['):
            value = '[' + value
        if not value.endswith(']'):
            value = value + ']'
        value=value.replace('""', '"')
        return value

    # Register the UDF
    ensure_brackets_udf = F.udf(ensure_brackets)

    # Apply the UDF to the 'contact_data' column
    df = df.withColumn("contact_data", ensure_brackets_udf("contact_data"))

    # Define the schema of the array elements for the contact_data
    contact_schema = ArrayType(StructType([
        StructField("contact_name", StringType(), True),
        StructField("contact_surname", StringType(), True),
        StructField("city", StringType(), True),
        StructField("cp", StringType(), True),
    ]))

    # Use 'from_json' to parse the column into an array of struct
    df = df.withColumn("contact_data", F.from_json(F.col("contact_data"), contact_schema))
    return df




def read_csv_from_source(csv_file_path=None):
    try:
        #validated_df=""
        # Define schema explicitly (optional but recommended for better performance)
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("date", StringType(), True),
            StructField("company_id", StringType(), True),
            StructField("company_name", StringType(), True),
            StructField("crate_type", StringType(), True),
            StructField("contact_data", StringType(), True),  # JSON-like, can be processed later
            StructField("salesowners", StringType(), True)  # Can be split into an array later
        ])
        df = spark.read.csv(csv_file_path, schema=schema, header=True, sep=";")
        validated_df=data_validation(df)
    except Exception as e:
        print(f"Error: {e}")
    return validated_df



def read_json_from_source(json_file_path=None):
    try:
        json_dict = ""
        with open(json_file_path, 'r') as f:
            json_dict = json.load(f)
    except Exception as e:
        print(f"Error: {e}")
    return json_dict




orders_csv_file_path = "resources/orders.csv"
invoice_json_file_path = "resources/invoicing_data.json"
orders_df=read_csv_from_source(orders_csv_file_path)
invoice_dict=read_json_from_source(invoice_json_file_path)



def test_data_csv():
    orders_csv_file_path_test = "test_resources/orders_test.csv"
    orders_df_test=read_csv_from_source(orders_csv_file_path_test)
    return orders_df_test



def test_data_json():
    invoice_json_file_path_test = "test_resources/invoicing_data_test.json"
    invoice_dict_test=read_json_from_source(invoice_json_file_path_test)
    return invoice_dict_test



def crate_distribution(df):
    # Group by company_name and crate_type, and count the number of occurrences
    crate_distribution = df.groupBy('company_name', 'crate_type').agg(F.count('*').alias('order_count'))
    return crate_distribution



def order_with_name_of_contact(df):
    # Define a UDF to handle full name extraction with 'John Doe' as the fallback
    df_with_full_name = df.withColumn(
        "contact_full_name",
        F.when(
            (F.size(F.col('contact_data')) > 0) &  # Check if contact_data is not empty
            F.col('contact_data')[0].getItem('contact_name').isNotNull() &  # Check if contact_name is not null
            F.col('contact_data')[0].getItem('contact_surname').isNotNull(),  # Check if contact_surname is not null
            F.concat(
                F.col('contact_data')[0].getItem('contact_name'),
                F.lit(" "),
                F.col('contact_data')[0].getItem('contact_surname')
            )
        ).otherwise(F.lit('John Doe'))
    )
    return df_with_full_name[['order_id', 'contact_full_name']]




def orders_with_contact_address(df):
    # Safely extract city and postal code from the 'contact_data' array
    df_with_address = df.withColumn(
        "contact_address",
        F.concat(
            F.coalesce(F.col('contact_data')[0].getItem('city'), F.lit('Unknown')),  # city with default value 'Unknown'
            F.lit(", "),  # Separator between city and postal code
            F.coalesce(F.col('contact_data')[0].getItem('cp'), F.lit('UNK00'))  # postal code with default value 'UNK00'
        )
    )
    return df_with_address[['order_id', 'contact_address']]




def sales_commissions(df, json):
    # Assuming the following schema for the invoices
    invoice_schema = StructType([
        StructField("id", StringType(), True),
        StructField("orderId", StringType(), True),
        StructField("companyId", StringType(), True),
        StructField("grossValue", StringType(), True),  # In cents
        StructField("vat", StringType(), True)  # VAT in percentage
    ])
    # Convert the data to a DataFrame
    df_invoice = spark.createDataFrame(json.get("data").get("invoices"), invoice_schema)
    df_invoice = df_invoice.withColumn("grossValue", df_invoice["grossValue"].cast(IntegerType())) \
        .withColumn("vat", df_invoice["vat"].cast(IntegerType()))
    # Assuming df_orders contains order details and df_sales contains sales owners
    # Rename columns for clarity
    df_orders = df_invoice.withColumnRenamed("orderId", "order_id").withColumnRenamed("companyId", "company_id")
    df_sales = df.withColumnRenamed("company_id", "company_id_sales")  # Avoid duplicate column names in join

    # Join data on order_id
    df1 = df_orders.join(df_sales, on="order_id", how="inner")

    # Calculate net invoiced value (convert cents to euros) and considered vat value from input as percentage of gross value
    df1 = df1.withColumn("net_value", (col("grossValue") - (col("grossValue")*(col("vat")/100))) / 100)

    # Explode salesowners into separate rows with ranking
    df1 = df1.withColumn("salesowners_list", split(col("salesowners"), ", ")) \
        .withColumn("salesowner", explode(col("salesowners_list"))) \
        .withColumn("salesowner", trim(col("salesowner")))  # Remove leading/trailing spaces

    # Assign commission rates based on ranking
    df1 = df1.withColumn("rank", expr("array_position(salesowners_list, salesowner)"))

    df1 = df1.withColumn("commission",
                         expr("""
        CASE 
            WHEN rank = 1 THEN net_value * 0.06
            WHEN rank = 2 THEN net_value * 0.025
            WHEN rank = 3 THEN net_value * 0.0095
            ELSE 0
        END
    """)
                         )

    # Aggregate total commission per salesperson
    df_commissions = df1.groupBy("salesowner").agg(expr("sum(commission) as total_commission"))

    # Format to two decimal places and sort in descending order
    df_commissions = df_commissions.withColumn("total_commission", expr("round(total_commission, 2)")) \
        .orderBy(col("total_commission").desc())

    # Convert to list of [salesowner, total_commission]
    commission_list = df_commissions.rdd.map(lambda row: [row["salesowner"], row["total_commission"]]).collect()

    return commission_list



def companies_with_sales_owners(df):
    def create_company_salesowners_df(df):
        # Split the salesowners string into an array of salesowners
        df = df.withColumn('salesowners_list', F.split(df['salesowners'], ', '))

        # Explode the list so each salesperson gets a separate row
        df_exploded = df.withColumn('salesowners_list', F.explode(df['salesowners_list']))

        # Remove rows where 'salesowners_list' is null or empty
        df_exploded = df_exploded.filter(df_exploded['salesowners_list'].isNotNull() & (df_exploded['salesowners_list'] != ''))

        # Group by company_id and company_name, and aggregate the sales owners
        df_aggregated = df_exploded.groupBy('company_id', 'company_name').agg(F.collect_set('salesowners_list').alias('salesowners_list'))

        # Sort salesowners alphabetically and convert back to a comma-separated string
        df_aggregated = df_aggregated.withColumn('list_salesowners', F.concat_ws(', ', F.sort_array(df_aggregated['salesowners_list'])))

        # Drop the salesowners_list column as we no longer need it
        df_aggregated = df_aggregated.drop('salesowners_list')

        return df_aggregated

    # Applying the transformation
    company_salesowners_df = create_company_salesowners_df(df)
    # Show the final result
    company_salesowners_df=company_salesowners_df.select('company_id', 'company_name', 'list_salesowners')
    return company_salesowners_df



#Test 1: Distribution of Crate Type per Company
print("#Test 1: Distribution of Crate Type per Company as below:")
df_distribution=crate_distribution(orders_df)
#Test for correct count
assert crate_distribution(test_data_csv()).filter(F.col("company_name") == "Fresh Fruits Co").filter(F.col("crate_type") == "Metal").collect()[0]["order_count"] == 1, "Wrong result!"
df_distribution.show()


# Test 2: DataFrame of Orders with Full Name of the Contact
print("Test 2: DataFrame of Orders with Full Name of the Contact as below:")
df_1=order_with_name_of_contact(orders_df)
#Test for concatenated name
assert order_with_name_of_contact(test_data_csv()).filter(F.col("order_id") == "order_3").collect()[0]["contact_full_name"] == "Con Than", "Wrong result!"
#Test for placeholder name
assert order_with_name_of_contact(test_data_csv()).filter(F.col("order_id") == "order_5").collect()[0]["contact_full_name"] == "John Doe", "Wrong result!"
df_1.show()


#Test 3: DataFrame of Orders with Contact Address
print("Test 3: DataFrame of Orders with Contact Address as below:")
df_2=orders_with_contact_address(orders_df)
#Test for address when both city name and postal code is available
assert orders_with_contact_address(test_data_csv()).filter(F.col("order_id") == "order_3").collect()[0]["contact_address"] == "City_3, 393", "Wrong result!"
#Test for address when either name and postal code is null
assert orders_with_contact_address(test_data_csv()).filter(F.col("order_id") == "order_2").collect()[0]["contact_address"] == "City_2, UNK00", "Wrong result!"
#Test for address when both name and postal code is null
assert orders_with_contact_address(test_data_csv()).filter(F.col("order_id") == "order_5").collect()[0]["contact_address"] == "Unknown, UNK00", "Wrong result!"
df_2.show()



# Test 4: Calculation of Sales Team Commissions
print("Test 4: Calculation of Sales Team Commissions as below:")
commissions_list_with_name=sales_commissions(orders_df, invoice_dict)
assert sales_commissions(test_data_csv(),test_data_json())[0][1] == 202.95, "Wrong result!"
assert sales_commissions(test_data_csv(),test_data_json())[0][1] >=  sales_commissions(test_data_csv(),test_data_json())[1][1] , "Wrong result!"
print(commissions_list_with_name)


#Test 5: DataFrame of Companies with Sales Owners
print("Test 5: DataFrame of Companies with Sales Owners as below:")
df_3=companies_with_sales_owners(orders_df)
df_3.show()
#Test for correct sorted order
assert companies_with_sales_owners(test_data_csv()).filter(F.col("company_name") == "Fresh Fruits Co").collect()[0]["list_salesowners"].split(", ")==sorted(['Owner c2', 'Owner c3', 'Owner C1'],key=str.lower), "Wrong result!"


#Test 6: Data Visualization (you don't need to submit a solution to this test if not explicitly asked for)
print("Test 6: Data Visualization :")
# Aggregate the data to count orders by crate_type
crate_distribution = orders_df.groupBy("crate_type").agg(F.count("order_id").alias("order_count"))

# Show the results (or visualize using matplotlib, seaborn, or any BI tool)
# crate_distribution.show()


import matplotlib.pyplot as plt

# Convert the crate_distribution to Pandas for plotting
crate_distribution_pd = crate_distribution.toPandas()

# Plot a pie chart for the distribution
plt.figure(figsize=(7, 7))
plt.pie(crate_distribution_pd['order_count'], labels=crate_distribution_pd['crate_type'], autopct='%1.1f%%', startangle=90)
plt.title('Distribution of Orders by Crate Type')
plt.show()







from pyspark.sql.functions import explode, split
from pyspark.sql.functions import col, date_sub, current_date
from pyspark.sql.window import Window

# Explode the salesowners column by splitting the comma-separated values
df_exploded = orders_df.withColumn("salesowner", explode(split(col("salesowners"), ",")))






# Filter for the last 12 months and plastic crates
last_12_months_df = df_exploded.filter(col("date") >= date_sub(current_date(), 365))
plastic_crates_df = last_12_months_df.filter(col("crate_type") == "Plastic")

# Aggregate by salesowner and count orders
salesowner_training = plastic_crates_df.groupBy("salesowner").agg(F.count("order_id").alias("order_count"))

# Order by order_count in ascending order to identify those needing the most training
salesowner_training = salesowner_training.orderBy("order_count")

# Convert to Pandas for plotting
salesowner_training_pd = salesowner_training.toPandas()

# Plot a bar chart for sales owners needing training
plt.figure(figsize=(10, 6))
plt.barh(salesowner_training_pd['salesowner'], salesowner_training_pd['order_count'], color='skyblue')
plt.xlabel('Number of Plastic Crates Sold')
plt.ylabel('Sales Owners')
plt.title('Sales Owners Needing Training Based on Plastic Crates Sold (Last 12 Months)')
plt.gca().invert_yaxis()  # Invert y-axis to show the lowest performers at the top
plt.show()
plt.savefig("output2.png")



# Filter for plastic crates
plastic_crates_df = df_exploded.filter(col("crate_type") == "Plastic")

# Extract year and month from the date to group by month
plastic_crates_df = plastic_crates_df.withColumn("year_month", F.date_format("date", "yyyy-MM"))

# Aggregate by salesowner and month
monthly_sales = plastic_crates_df.groupBy("salesowner", "year_month").agg(F.count("order_id").alias("monthly_order_count"))

# Define a rolling window of 3 months
window_spec = Window.partitionBy("salesowner").orderBy("year_month").rowsBetween(-2, 0)

# Calculate the rolling 3-month total
monthly_sales = monthly_sales.withColumn("rolling_3_months_sales", F.sum("monthly_order_count").over(window_spec))

# Get the top 5 performers by rolling 3 months sales
top_5_performers = monthly_sales.orderBy("rolling_3_months_sales", ascending=False).limit(5)

# Convert to Pandas for plotting
top_5_performers_pd = top_5_performers.toPandas()

# Plot a bar chart for top 5 performers
plt.figure(figsize=(10, 6))  # Adjust figure size to ensure space for labels
plt.barh(top_5_performers_pd['salesowner'], top_5_performers_pd['rolling_3_months_sales'], color='lightgreen')

# Adjust the plot labels and title for better visualization
plt.xlabel('3-Month Rolling Sales (Plastic Crates)')
plt.ylabel('Sales Owners')
plt.title('Top 5 Performers Selling Plastic Crates (Rolling 3-Month Evaluation)')
plt.gca().invert_yaxis()  # Invert y-axis to show the top performer at the top

# Ensure the labels are visible and not cut off
plt.tight_layout()

# Show the plot
plt.show()
plt.savefig("output3.png")





# Stop Spark Session
spark.stop()
