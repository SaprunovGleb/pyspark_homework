from pyspark.sql import DataFrame


class SchemaMerging:
    """
    Class provides possibilities to union tow datasets with different schemas.
    Result dataset should contain all rows from both with columns from both dataset.
    If columns have the same name and type - they are identical.
    If columns have different types and the same name, 2 new column should be provided with next pattern:
    {field_name}_{field_type}
    """

    # ToDo: Implement dataset union with schema merging
    def union(self, dataframe1: DataFrame, dataframe2: DataFrame):
        df1 = dataframe1
        df2 = dataframe2
        df1_names=df1.columns
        df2_names=df2.columns
        key = []
        #df1.printSchema()
        #df2.printSchema()
        for df1_name in df1_names:
            for df2_name in df2_names:
                if df1_name == df2_name:
                    if df1.schema[df1_name].dataType != df2.schema[df2_name].dataType:
                        df1 = df1.withColumnRenamed(df1_name, df1_name +"_"+ str(df1.schema[df1_name].dataType))
                        df2 = df2.withColumnRenamed(df2_name, df2_name +"_"+ str(df2.schema[df2_name].dataType))
                    else:
                        key.append(df1_name)
        #print(key)
        #print(df1.schema[0].dataType)
        if len(key) > 0:
            df3 = df1.join(df2, key, "full").sort(key)
        else:
            df3 = df1.join(df2,(df1[df1_names[0]] == df2[df2_names[0]])&(df1[df1_names[0]] != df2[df2_names[0]]),how = "full")
        return df3
