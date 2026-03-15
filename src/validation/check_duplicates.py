
def check_duplicates(df, columns):

    duplicate_df = df.groupBy(columns).count().filter("count > 1")

    duplicate_count = duplicate_df.count()

    if duplicate_count > 0:
        print(f"Duplicate records found: {duplicate_count}")
        duplicate_df.show()
    else:
        print("No duplicate records found.")
