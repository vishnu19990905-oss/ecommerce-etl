from pyspark.sql.functions import *

def check_nulls(df, columns):

    for column in columns:
        null_count = df.filter(col(column).isNull()).count()

        if null_count > 0:
            print(f"Column {column} has {null_count} null values")
        else:
            print(f"Column {column} has no null values")
