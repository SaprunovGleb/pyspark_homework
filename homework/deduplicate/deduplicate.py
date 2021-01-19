from typing import Union, List

from pyspark.sql import DataFrame


class Deduplicator:
    """
    Current class provides possibilities to remove duplicated rows in data depends on provided primary key.
    If no primary keys were provided should be removed only identical rows.
    """

    # ToDo: Implement this method
    def deduplicate(self, primary_keys: Union[str, List[str]], dataframe: DataFrame) -> DataFrame:
        if isinstance(primary_keys, str):
            if primary_keys == "":
                dfout = dataframe.dropDuplicates()
            else:
                key = []
                key.append(primary_keys)
                dfout = dataframe.dropDuplicates(key)
        else:
            if primary_keys == []:
                dfout = dataframe.dropDuplicates()
            else:
                dfout = dataframe.dropDuplicates(primary_keys)  
        return dfout
        