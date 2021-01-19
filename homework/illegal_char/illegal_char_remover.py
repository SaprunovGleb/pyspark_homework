from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f
from typing import List


class IllegalCharRemover:
    """
    Class provides possibilities to remove illegal chars from string column.
    """
    def __init__(self, chars: List[str], replacement):
        self.chars = chars
        self.replacement = replacement


    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        #print(self.chars)
        line = ""
        spesial = ['^', '\\', '/', '[', ']', '{', '}']
        for char in self.chars:
            #print(char)
            if spesial.count(char) != 0:
                line = line + "\\"
            line = line + char + "|"
        line = line[:-1]
        dataframe = dataframe.withColumn(target_column,\
            f.regexp_replace(source_column, line, self.replacement))\
            .drop(source_column)
        return dataframe



