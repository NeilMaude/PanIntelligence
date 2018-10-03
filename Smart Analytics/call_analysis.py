# Call analysis tools
# Neil Maude
# 2-Oct-2018
# Will process Smart call data and predict REPEAT calls from calls in the last X days

# Environment

import sys              # General system functions
import pyodbc           # Python ODBC library
import pandas as pd     # Pandas data frame library

from sklearn.preprocessing import StandardScaler                    # for scaling features to zero-mean/unit-variance
from sklearn.model_selection import train_test_split                # splitting of data into training/validation
from keras.utils.np_utils import to_categorical                     # one-hot encoding of outputs
from keras.models import Sequential                                 # Standard sequential model
from keras.layers.core import Dense, Activation, Dropout            # Usual layers required
from keras.optimizers import Adam                                   # Import the Adam optimiser
from keras.callbacks import EarlyStopping                           # Early stopping callback to avoid over-fit
# Constants
MODULE_NAME = 'call_analysis.py'
SERVER = 'wf-sql-01'
DATABASE = 'smart'
CONNECTION = 'DRIVER={SQL Server Native Client 11.0};'

REPEAT_DAYS = 14        # Number of calendar days to allow for a repeat call
TRAIN_SIZE = 0.8        # Percentage of training data to use for training, vs validation

# Field handling
# Categoricals are fields to 1-hot encode for deep learning
categoricals = ['BusinessType','Manufacturer', 'ProductId', 'DeviceType','LastOtherCallType','CreatedBy','CreatedDay',
                'AttendDay', 'PostCodeArea','FirstEngineer','SymptomCodeId']
# Drop fields are fields to dispose of from the dataframe, as not useful for prediction
drop_fields = ['ID', 'Incident', 'IncidentType', 'CustomerId', 'LedgerCode', 'SiteId',
               'SerialNo', 'SymptomDescription']
# Time stamp fields - these will also need to be dropped from the dataframe, they're already encoded
time_stamp_fields = ['FirstUsed','Installed','LastBreakCall','LastOtherCall','InitialMeterReadingDate',
                     'LastMeterReadingDate','CreatedDateTime','CreatedTime','AttendDateTime']

# Global variables
dbconn = None


# Helper function - just stick quotes around the string, for SQL building
def quote_string(sString):
    if sString.find("'") < 0:
        return "'%s'" % sString
    else:
        return "'%s'" % sString.replace("'", "#")

def writelog(message):

    global dbconn

    # write to the message log
    sql = 'INSERT INTO zCALL_ANALYSIS_LOG (Description) VALUES (' + quote_string(message) + ')'
    #print(sql)
    cursor = dbconn.cursor()
    cursor.execute(sql)
    cursor.commit()

def run_analysis(username, password):

    print('Starting analysis process')
    global dbconn

    # Connect to the database
    s_conn = CONNECTION + 'SERVER='+SERVER+';DATABASE='+DATABASE+';'
    s_conn += 'UID='+username+';PWD='+password
    dbconn = pyodbc.connect(s_conn)

    writelog('Python analysis started and connected to database OK')

    # Read in the data
    sql = 'select * from zCALL_ANALYSIS Order By [Incident] ASC'
    df_all = pd.read_sql_query(sql, dbconn)                         # can read SQL direct to a data frame
    print('Read %s rows, with %s columns' % (df_all.shape[0],df_all.shape[1]))

    # Encode the data
    # Need to set up unique names for values
    for c in categoricals:
        df_all[c] = df_all[c].map(lambda x: ((str(c)) + '-' + str(x)))
    # Now go over all of the columns and create one-hot encoding
    for c in categoricals:
        one_hot = pd.get_dummies(df_all[c])
        df_all = df_all.join(one_hot)
    print('Now have %s rows, with %s columns' % (df_all.shape[0], df_all.shape[1]))
    # Extract the ground-truth values
    #y_repeat = df_all['IncidentType'].map(lambda x: x == 'REPEAT')
    y_repeat = df_all['Repeated'].map(lambda x: x == 'YES')

    # Find the point at which predictions required (i.e recent incidents)
    sql = 'select MIN(Incident) [MinIncident] from zCALL_ANALYSIS '
    sql += 'where AttendDateTime >= DATEADD(d, -' + str(REPEAT_DAYS) + ', getdate())'
    cursor = dbconn.cursor()
    cursor.execute(sql)
    rs = cursor.fetchall()
    min_prediction_incident = rs[0].MinIncident

    print('Minimum incidentId for prediction: %s' % min_prediction_incident)

    min_prediction_index = df_all.shape[0]-1
    while (df_all['Incident'][min_prediction_index] > min_prediction_incident):
        min_prediction_index -= 1
    print('Found minimum incident at row %s, incidentId %s' % (min_prediction_index, df_all['Incident'][min_prediction_index]))

    # Retain the list of incident IDs, for reporting later
    df_incident = df_all['Incident']
    print('Retained incident list with %s rows' % (df_incident.shape[0]))

    # Drop out fields which are not required
    for c in categoricals + drop_fields + time_stamp_fields:
        del df_all[c]
    print('After dropping fields, now have %s rows, with %s columns' % (df_all.shape[0], df_all.shape[1]))

    # Get rid of any NULL values in numerical fields, replace any remaining NaN values with -1
    # This is to avoid breaking the algorithms later
    df_all = df_all.fillna(-1)

    # Scale the fields, zero-mean/unit-variance
    X_scaler = StandardScaler().fit(df_all)
    X_scaled = X_scaler.transform(df_all)

    # Encode the target outputs
    y_binary = to_categorical(y_repeat)

    # Split out the training data / test data / prediction data
    X_predict = X_scaled[min_prediction_index:]
    y_predict = y_binary[min_prediction_index:]                  # may use this for known REPEATS in window
    X_model   = X_scaled[0:min_prediction_index]
    y_model   = y_binary[0:min_prediction_index]

    X_train, X_validate, y_train, y_validate = train_test_split(X_model, y_model,
                                                                train_size=TRAIN_SIZE, test_size=1-TRAIN_SIZE)

    print('Created training set of size %s, validation set of size %s' % (len(X_train), len(X_validate)))
    print('Will predict on %s calls since cut-off date' % len(X_predict))
    print('Total records: %s' % (len(X_train)+len(X_validate)+len(X_predict)))

    # Create the network structure
    nn_model = Sequential()
    nn_model.add(Dense(df_all.shape[1], input_shape=(df_all.shape[1] * 1,)))
    nn_model.add(Activation('relu'))
    nn_model.add(Dropout(0.5))
    nn_model.add(Dense(1000))
    nn_model.add(Activation('relu'))
    nn_model.add(Dense(200))
    nn_model.add(Activation('relu'))
    nn_model.add(Dense(100))
    nn_model.add(Activation('relu'))
    nn_model.add(Dense(50))
    nn_model.add(Activation('relu'))
    nn_model.add(Dense(10))
    nn_model.add(Activation('relu'))
    nn_model.add(Dense(2))
    nn_model.add(Activation('softmax'))
    nn_model.compile(loss='categorical_crossentropy',
                     optimizer=Adam(),
                     metrics=['accuracy'])
    nn_model.summary()

    # Train a model, with early stopping
    nBatchSize = 32
    nEpoch = 50
    early_stop = EarlyStopping(monitor='val_loss',
                               min_delta=0,
                               patience=5,
                               verbose=0, mode='min')
    nn_model.fit(X_train, y_train,
                 batch_size=nBatchSize, epochs=nEpoch,
                 verbose=1, validation_data=(X_validate, y_validate), callbacks=[early_stop])

    # Make predictions (note that some calls may already be repeats by this time)

    # Done
    writelog('Python analysis process completed')
    print('\nAnalysis process completed')

if __name__ == "__main__":
  if len(sys.argv) < 3:
    print('Usage: python ' + MODULE_NAME + ' username password')
    sys.exit(1)
  else:
    run_analysis(sys.argv[1], sys.argv[2])