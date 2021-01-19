from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode, posexplode, posexplode_outer, lit, struct, arrays_zip, array, col


class UnpackNestedFields:
    """
    Class provides possibilities to unpack nested structures in row recursively and provide flat structure as result.
    To clarify rules, please investigate tests.
    After unpacking of structure additional columns should be provided with next name {struct_name}.{struct_field_name}

    """

    # ToDo implement unpacking of nested fields
    def unpack_nested(self, dataframe: DataFrame):
        unpacking= True
        while unpacking:
            schema = dataframe.schema.fields
            columns= dataframe.columns
            #print(columns)
            n = len(schema)
            i = 0
            while i < n:
                plase_Arr = str(schema[i]).find("Array")
                plase_List = str(schema[i]).find("List")
                if plase_Arr != -1 and ((plase_Arr < plase_List) or plase_List == -1):
                    dataframe = dataframe.select("*",explode(dataframe.columns[i]))\
                        .drop(columns[i]).withColumnRenamed('col',columns[i])
                    break
                elif str(schema[i]).find("List") != -1:
                    arrayIn = dataframe.schema[columns[i]].dataType.names
                    j = 0
                    m = len(arrayIn)
                    while j < m:
                        #   print(arrayIn[j])
                        dataframe = dataframe.withColumn((columns[i] + arrayIn[j]),col(columns[i])[arrayIn[j]])
                        j = j + 1
                    dataframe = dataframe.drop(columns[i])
                    break 
                #print(i)
                i += 1
            else:
                unpacking=False
            return dataframe