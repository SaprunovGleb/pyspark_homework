from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

import pyspark
from pyspark.sql.types import *
class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):
        dataframe = dataframe.rdd.toDF(expected_schema)
# =============================================================================
#         odl_schema = dataframe.schema.fields
#         new_schema = expected_schema.fields
#         n = len(odl_schema)
#         if len (new_schema)!= n:
#             return -1
#         i = 0
#         while i < n:
#             if odl_schema[i] != new_schema[i]:
#                 print(str(odl_schema[i]),str(new_schema[i]))
#                 old_array = (str(odl_schema[i])[str(odl_schema[i]).find("(")+1:-1]).split(",")
#                 new_array = (str(new_schema[i])[str(new_schema[i]).find("(")+1:-1]).split(",") 
#                 #old_array[1] += "()"
#                 #new_array[1] += "()"
#                 try:
#                   if i == 2:
#                       dataframe = dataframe.withColumn(new_array[0],dataframe[old_array[0]].cast(DateType))
#                   if old_array[0] == new_array[0]:   
#                       print(i)
#                      dataframe = dataframe.withColumn(new_array[0],dataframe[old_array[0]].cast(new_array[1]))
#                   else:
#                       dataframe = dataframe.withColumn(new_array[0],dataframe[old_array[0]].cast(new_array[1]))\
#                           .drop(old_array[0])           
#                 except:
#                     print(new_array[1], " in column ", new_array[0], "is not avilubal")
#             i += 1
# =============================================================================
            
        return dataframe
