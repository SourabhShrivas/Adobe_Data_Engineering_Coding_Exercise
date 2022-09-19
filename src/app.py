import sys
from boto3 import Session
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from urllib.parse import urlparse, parse_qsl

# initiate spark session
spark = SparkSession.builder.getOrCreate()

today_date = datetime.today().strftime("%Y-%m-%d")

class keyword_performance:
    def __init__(self, x):
        """
        Function to initialize the class with the input data file
        Args:
            x (file path): Input file path in Aws S3 location
        Returns:
            DataFrame: Dataframe object containing the data
        """
        self.df = spark.read.csv(x,
                                 sep=r"\t",
                                 header=True)

    def process_etl(self):
        """
        Function to process the data and perform ETL transformations. following are the trasnformations - 
        1. Parsing the domain name from the given URL
        2. Parsing the search keyword from the given URL
        3. Parsing the revenue for the product corresponding to the search keyword from the products list 
        4. Calculating the total revenue for the combination of search keyword and domain name
        
        Returns:
            DataFrame: Dataframe object containing the metrics such as total revenue
        """

        def domain_name_parser(url):
            """
            Helper function to parse the domain name from the given URL
            Args:
                url (String): url for the search engine
            Returns:
                Series: Series object containing the domain name
            """
            a = urlparse(url)
            if a.netloc.split(".")[1] in ("google", "yahoo", "bing"):
                return ".".join(a.netloc.split(".")[1:])

        def keyword_parser(url):
            """
            Helper function to parse the search keyword from the given URL
            Args:
                url (String): url for the search engine
            Returns:
                Series: Series object containing the search keyword
            """
            a = urlparse(url)
            if "yahoo" in url:
                return dict(parse_qsl(a.query))["p"]
            elif "google" in url or "bing" in url:
                return dict(parse_qsl(a.query))["q"]
            else:
                return None

        def revenue_parser(products, search_keyword):
            """
            Helper function to parse the revenue for the product corresponding to the 
            search keyword from the given product list
            Args:
                products (List): list of products with their attributes
                search_keyword (String): 
            Returns:
                Series: Series object containing the revenue
            """
            if products is None:
                return None
            else:
                product_list = products.split(",")
                for each in product_list:
                    if search_keyword.lower() in each.split(";")[1].lower():
                        return each.split(";")[3]
                    else:
                        return None
        
        # creating the udfs for all the helper functions
        udf_domain_name_parser = F.udf(lambda x: domain_name_parser(x), StringType())
        udf_keyword_parser = F.udf(lambda x: keyword_parser(x), StringType())
        udf_revenue_parser = F.udf(lambda x, y: revenue_parser(x, y), StringType())
        
        # parsing the search keyword, domain name and revenue
        df = self.df.withColumn(
            "search_keyword", udf_keyword_parser(F.col("referrer"))
        ).withColumn("search_engine_domain", udf_domain_name_parser(F.col("referrer")))

        window_spec = Window.partitionBy(F.col("ip")).orderBy(F.col("date_time"))

        df = (
            df.withColumn(
                "search_keyword_forwardfill",
                F.lower(F.last("search_keyword", True).over(window_spec)),
            )
            .withColumn(
                "search_engine_domain_forwardfill",
                F.last("search_engine_domain", True).over(window_spec),
            )
            .withColumn(
                "revenue",
                udf_revenue_parser(
                    F.col("product_list"), F.col("search_keyword_forwardfill")
                ),
            )
        )
        
        # calculating the total revenue for the for the products referred by search engine
        df = (
            df.filter(F.col("event_list") == 1)
            .groupby("search_engine_domain_forwardfill", "search_keyword_forwardfill")
            .agg(F.sum("revenue").alias("revenue"))
            .orderBy(F.col("revenue").desc())
            .select(
                F.col("search_engine_domain_forwardfill").alias("search engine domain"),
                F.col("search_keyword_forwardfill").alias("search keyword"),
                "revenue",
            )
        )
        return df
    
def main():
    """
    Main function which initializes the class and execute the etl process
    """
    args = getResolvedOptions(sys.argv, ["VAL1","VAL2"])
    file_name=args['VAL1']
    bucket_name=args['VAL2']
    print("Bucket Name" , bucket_name)
    print("File Name" , file_name)
    input_file_path="s3a://{}/{}".format(bucket_name,file_name)
    print("Input File Path : ",input_file_path)
    
    etl = keyword_performance(input_file_path)
    df = etl.process_etl()
    df.coalesce(1).write.option("header", True).option("delimiter", r"\t").csv(
        "s3a://outputs3bucket-adobe/{}_SearchKeywordPerformance.tab".format(
            today_date
        ),
        mode="overwrite",
    )
    print("ETL process successfully completed and output file is loaded to adobe output S3 bucket")

if __name__ == "__main__":
   main()