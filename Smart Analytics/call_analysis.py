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
import numpy as np                                                  # Numerical analysis tools

from datetime import datetime                                       # for date processing
from datetime import timedelta                                      # for date processing

# Constants
MODULE_NAME = 'call_analysis.py'
SERVER = 'wf-sql-01'
DATABASE = 'smart'
CONNECTION = 'DRIVER={SQL Server Native Client 11.0};'

START_DATE = '01/01/2017'   # Start date for modelling data - can be used to control training time (e.g. dynamic 1 year)
REPEAT_DAYS = 14            # Number of calendar days to allow for a repeat call
TRAIN_SIZE = 0.8            # Percentage of training data to use for training, vs validation
THRESHOLD_SAMPLE_SIZE = 1000    # Sample size for determining desired threshold

# Field handling
# Categoricals are fields to 1-hot encode for deep learning
categoricals = ['BusinessType','Manufacturer', 'ProductId', 'DeviceType','LastOtherCallType','CreatedBy','CreatedDay',
                'AttendDay', 'PostCodeArea','FirstEngineer','SymptomCodeId']
# Drop fields are fields to dispose of from the dataframe, as not useful for prediction
drop_fields = ['Repeated', 'ID', 'Incident', 'IncidentType', 'CustomerId', 'LedgerCode', 'SiteId',
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

# Load, scale and split out the data into training/prediction sets
def load_data(dbConn, startdate, predictdate):
    # Dates expected to be in the form dd/mm/yyyy
    d_startdate = datetime.strptime(startdate, '%d/%m/%Y')
    d_predictdate = datetime.strptime(predictdate, '%d/%m/%Y')
    d_enddate = d_predictdate + timedelta(days=REPEAT_DAYS)

    # Read in the data
    sql = 'select * from zCALL_ANALYSIS '
    sql += 'where [AttendDateTime] >= ' + quote_string('{:%d-%B-%Y}'.format(d_startdate))
    sql += ' and [AttendDateTime] <= ' + quote_string('{:%d-%B-%Y}'.format(d_enddate))
    sql += ' Order By [AttendDateTime] ASC'

    print('Using load SQL:\n',sql)

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
    sql += 'where AttendDateTime >= ' + quote_string('{:%d-%B-%Y}'.format(d_predictdate))

    print('Using predict date SQL:\n', sql)

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

    return X_model, y_model, X_predict, y_predict

# Build a neural net on the sample data
def build_model(X_model, y_model):
    # Model data shape
    print('Model data shape: ', X_model.shape)

    # now split out for training/validation (randomly)
    X_train, X_validate, y_train, y_validate = train_test_split(X_model, y_model,
                                                                train_size=TRAIN_SIZE, test_size=1-TRAIN_SIZE)

    print('Created training set of size %s, validation set of size %s' % (len(X_train), len(X_validate)))

    # Create the network structure
    nn_model = Sequential()
    nn_model.add(Dense(X_model.shape[1], input_shape=(X_model.shape[1] * 1,)))
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

    return nn_model

# Determine threshold to use, with some ratio of true positives to false positives
def find_threshold(nn_model, X_threshold, y_threshold):

    required_ratio = 5.0        # TP to FP ratio
    best_threshold_value = 0.0
    best_ratio = 0.0
    f_found_solution = False

    # inputs are some data with known ground-truth, but not seen by the network in training
    predictions = nn_model.predict(X_threshold)
    predicted_repeats = np.argmax(predictions, axis=1)

    for t in range(1,100,1):
        threshold = float(t) / 100       # 100 steps to test
        true_positive = 0
        false_positive = 0
        true_negative = 0
        false_negative = 0
        for i in range(len(predictions)):
            if y_threshold[i][1] > 0.1:
                # the ground truth is a repeat
                if predictions[i][1] >= threshold:
                    true_positive += 1
                else:
                    false_negative += 1
            else:
                # the ground truth is not a repeat
                if predictions[i][1] >= threshold:
                    false_positive += 1
                else:
                    true_negative += 1
        print('Threshold setting = %.2f' % threshold)
        print('Results are:')
        print(' True positives  - correct predictions:   %s' % true_positive)
        print(' True negatives  - correct predictions:   %s' % true_negative)
        print(' False positives - incorrect predictions: %s' % false_positive)
        print(' False negatives - incorrect predictions: %s' % false_negative)
        # Can we accept this threshold?
        if false_positive == 0:
            # no false positives!
            print('No false positives at threshold = %s' % threshold)
            if f_found_solution == False:
                best_threshold_value = threshold
                f_found_solution = True
        elif (float(true_positive) / false_positive > best_ratio) and f_found_solution == False:
            best_ratio = float(true_positive) / false_positive
            print('New best ratio TP/FP = %s at threshold = %s' % (best_ratio,threshold))
        if (best_ratio >= required_ratio) and f_found_solution == False:
            # acceptable level
            best_threshold_value = threshold
            f_found_solution = True

    return best_threshold_value

def run_analysis(username, password):

    print('Starting analysis process')
    global dbconn

    # Connect to the database
    s_conn = CONNECTION + 'SERVER='+SERVER+';DATABASE='+DATABASE+';'
    s_conn += 'UID='+username+';PWD='+password
    dbconn = pyodbc.connect(s_conn)

    writelog('Python analysis started and connected to database OK')

    predict_date = datetime.now() - timedelta(days=REPEAT_DAYS)
    s_predict_date = '{:%d/%m/%Y}'.format(predict_date)
    X_model, y_model, X_predict, y_predict = load_data(dbconn, START_DATE, s_predict_date)

    # Retain some data for threshold selection
    X_training = X_model[0:len(X_model)-THRESHOLD_SAMPLE_SIZE]
    y_training = y_model[0:len(y_model)-THRESHOLD_SAMPLE_SIZE]
    X_threshold = X_model[len(X_model)-THRESHOLD_SAMPLE_SIZE:]
    y_threshold = y_model[len(y_model)-THRESHOLD_SAMPLE_SIZE:]
    print('Total records: %s' % (len(X_training)+len(X_threshold)+len(X_predict)))

    nn_model = build_model(X_training, y_training)

    # Find threshold values
    threshold = find_threshold(nn_model, X_threshold, y_threshold)
    print('Best threshold found at: %s' % threshold)

    print('Will predict on %s calls since cut-off date' % len(X_predict))




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