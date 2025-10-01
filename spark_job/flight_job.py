from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import when,col,lit,expr,count,avg

def main_process(env,bq_dataset,transformed_table,route_insights_table,origin_insights_table,bq_project):
    spark = SparkSession.builder \
        .appName("FlightBookingDataProcessing") \
        .getOrCreate()
    
    inputpath=f"gs://airflow_demo_data/fighlt_data/source-{env}"

    data = spark.read.csv(inputpath, header=True, inferSchema=True)\
    
    transformed_data = data.withColumn(
            "is_weekend", when(col("flight_day").isin("Sat", "Sun"), lit(1)).otherwise(lit(0))
        ).withColumn(
            "lead_time_category", when(col("purchase_lead") < 7, lit("Last-Minute"))
                                  .when((col("purchase_lead") >= 7) & (col("purchase_lead") < 30), lit("Short-Term"))
                                  .otherwise(lit("Long-Term"))
        ).withColumn(
            "booking_success_rate", expr("booking_complete / num_passengers")
        )
    route_insights = transformed_data.groupBy("route").agg(
            count("*").alias("total_bookings"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("length_of_stay").alias("avg_stay_length")
        )

    booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_bookings"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")
        )
    transformed_data.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{transformed_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

        # Write route insights to BigQuery
    
    route_insights.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{route_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()

        # Write booking origin insights to BigQuery
       
    booking_origin_insights.write \
            .format("bigquery") \
            .option("table", f"{bq_project}:{bq_dataset}.{origin_insights_table}") \
            .option("writeMethod", "direct") \
            .mode("overwrite") \
            .save()
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process flight booking data and store insights in BigQuery.")
    parser.add_argument("--env", required=True, help="Environment (e.g., dev, prod)")
    parser.add_argument("--bq_dataset", required=True, help="BigQuery dataset name")
    parser.add_argument("--transformed_table", required=True, help="BigQuery table for transformed data")
    parser.add_argument("--route_insights_table", required=True, help="BigQuery table for route insights")
    parser.add_argument("--origin_insights_table", required=True, help="BigQuery table for booking origin insights")
    parser.add_argument("--bq-project", required=True, help="GCP Project ID")

    args = parser.parse_args()
    
    main_process(
        env=args.env,
        bq_dataset=args.bq_dataset,
        transformed_table=args.transformed_table,
        route_insights_table=args.route_insights_table,
        origin_insights_table=args.origin_insights_table,
        bq_project=args.bq_project
    )

  