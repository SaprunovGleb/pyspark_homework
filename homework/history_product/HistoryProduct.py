from pyspark.sql import DataFrame, functions

colsnamesn = []
colsnameso = []

class HistoryProduct:
    """
    Class provides possibilities to compute history of rows.
    You should compare old and new dataset and define next for each row:
     - row was changed
     - row was inserted
     - row was deleted
     - row not changed
     Result should contain all rows from both datasets and new column `meta`
     where status of each row should be provided ('not_changed', 'changed', 'inserted', 'deleted')
    """
    def __init__(self, primary_keys=None):
        self.primary_keys = primary_keys
        

    
    # ToDo: Implement history product
    def get_history_product(self, old_dataframe: DataFrame, new_dataframe: DataFrame):
        global colsnames 
        colsnames = old_dataframe.columns
        global colsnameso 
        colsnameso = list()
        colsnamesn = list()
        colsnamesKO = list()
        primary_keys = self.primary_keys
        for name in colsnames:
            if not (primary_keys is None):
                if primary_keys.count(name) == 0:
                    colsnamesn.append(name)
                    name = name + '_Old'
                    colsnameso.append(name)
            else:
                colsnamesn.append(name)
                name = name + '_Old'
                colsnameso.append(name)            
            colsnamesKO.append(name)

        #print(colsnamesn)
        #print(colsnameso)
        #print(colsnamesKO)
        n = len(colsnamesn)
        if not (primary_keys is None):
            old_dataframe = old_dataframe.toDF(*colsnamesKO)
            dataframejoin = new_dataframe.join(old_dataframe, primary_keys, 'outer')
        else: 
            dataframejoin = new_dataframe.join(old_dataframe,colsnamesn, 'outer')
            old_dataframe = old_dataframe.toDF(*colsnamesKO)
            dataframejoin = new_dataframe.join(old_dataframe,new_dataframe[colsnamesn[0]]==old_dataframe[colsnameso[0]], 'outer')
        dataframejoin = dataframejoin.withColumn("new_isnull",functions.lit(True))
        #dataframejoin.show()
        for name in colsnamesn:
            dataframejoin = dataframejoin.withColumn("new_isnull",dataframejoin["new_isnull"] & dataframejoin[name].isNull())
            #dataframejoin.show()
        dataframejoin = dataframejoin.withColumn("old_isnull",functions.lit(True))
        for name in colsnameso:
            dataframejoin = dataframejoin.withColumn("old_isnull",dataframejoin["old_isnull"] & dataframejoin[name].isNull())
            #dataframejoin.show()
        dataframejoin = dataframejoin.withColumn("oldnew_isthesame",functions.lit(True))
        i = 0
        while i < n:
            dataframejoin = dataframejoin.withColumn("oldnew_isthesame",\
                dataframejoin["oldnew_isthesame"] & 
                ((dataframejoin[colsnamesn[i]] == dataframejoin[colsnameso[i]]) |
                 (dataframejoin[colsnamesn[i]].isNull() & 
                  dataframejoin[colsnameso[i]].isNull())))
            i += 1
        dataframejoin = dataframejoin.fillna( {'oldnew_isthesame': False})
        i = 0
        while i < n:
            dataframejoin = dataframejoin.withColumn(colsnamesn[i],functions.when(dataframejoin["new_isnull"],dataframejoin[colsnameso[i]]).otherwise(dataframejoin[colsnamesn[i]]))
            #dataframejoin.show()
            i += 1
        dataframejoin = dataframejoin.withColumn('meta',\
                        functions.when(dataframejoin["new_isnull"],'deleted')\
            .when(dataframejoin["old_isnull"], 'inserted')\
            .when(dataframejoin["oldnew_isthesame"], 'not_changed').otherwise('changed'))
        #dataframejoin.show()
        for name in colsnameso:
             dataframejoin = dataframejoin.drop(name)
        dataframejoin = dataframejoin.drop("new_isnull").drop("old_isnull").drop("oldnew_isthesame").orderBy(colsnames)
        #dataframejoin.show()
        return dataframejoin


