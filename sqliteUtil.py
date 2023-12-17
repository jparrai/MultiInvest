import numpy as np
import sqlite3 as sl


# Class for defining an aggregate function
# to calculate the geometric average of the selected values in a sqlite
# Will discard all NULL or zero values, there are other ways of treating this values
# Will return None if no values or the geometric mean of the non-zero values
# numpy as np has to be imported
# USAGE: sqliteConnection.create_aggregate("g_avg", 1, GeometricAvg)
#        cursor.execute('SELECT g_avg(value) FROM data')
class GeometricAvg:
    def __init__(self):
        self.counter = 0
        self.mean = 0.0

    def step(self, value):
        if type(value) == int or type(value) == float:
            if value is not None and value > 0:
                self.mean += np.log(value)
                self.counter += 1

    def finalize(self):
        if self.counter > 0:
            return np.exp(self.mean / self.counter)
        else:
            return None


# Class for defining an aggregate function
# to calculate the Pearson's correlation of the selected values in a sqlite query
# numpy as np has to be imported
# Will discard all NULL values in any of the columns
# Will return 0 if no valid values
# USAGE: sqliteConnection.create_aggregate("corr", 2, PearsonsCorr)
#        cursor.execute('SELECT corr(column1, column2) FROM data')
class PearsonsCorr:
    def __init__(self):
        self.values = []
        self.corrs = []

    def step(self, value, corr):
        print(value, corr)
        if value is not None and corr is not None:
            self.values.append(value)
            self.corrs.append(corr)

    def finalize(self):
        return np.corrcoef(self.values, self.corrs)[0, 1] if len(self.values) > 0 else 0


def deleteTable(cursor: sl.Cursor, table):
    return None
