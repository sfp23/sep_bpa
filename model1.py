from keras.models import Sequential, model_from_json
from keras.layers import Dense, Activation, Dropout, Flatten, TimeDistributed
from keras.layers.recurrent import LSTM
from keras.optimizers import Adam
from sklearn.metrics import log_loss, precision_recall_curve, roc_curve, classification_report, precision_score, recall_score, average_precision_score, roc_auc_score, confusion_matrix
import numpy as np
import os
import random
import fnmatch
import h5py
import mysql.connector
import re
import argparse

#parser = argparse.ArgumentParser()
#parser.add_argument('offset')
#args = parser.parse_args()
#
#try:
#	HOUR_OFFSET = int(args.offset)
#except ValueError:
#	print('Argument value error')
#	sys.exit()

HOUR_OFFSET = 0

# Loading data

episodes = []
inputs = []

# Loading cases
os.chdir('/labs/shahlab/spoole/Sepsis/tensors/cases')
for file in os.listdir('.'):
	if fnmatch.fnmatch(file, '*_features.npy'):
		episodes.append(int(re.search('_(.*?)_', file).group(1)))
		inputs.append(np.load(file))

# Loading controls
os.chdir('/labs/shahlab/spoole/Sepsis/tensors/controls')
for file in os.listdir('.'):
	if fnmatch.fnmatch(file, '*_features.npy'):
		episodes.append(int(re.search('_(.*?)_', file).group(1)))
		inputs.append(np.load(file))

os.chdir('/labs/shahlab/spoole/Sepsis/models')
con = mysql.connector.connect(host = 'ncbolabs-db1.stanford.edu', user = 'spoole', passwd = '***')
curs = con.cursor()

# Finding actual bpa times (relative to start of episode)
query = 'select episode, bpa_start_time - episode_start_time as bpa_start, bpa_end_time - episode_start_time as bpa_end from user_spoole.sepsis_subset_cases;'
numrows = curs.execute(query)
temp = curs.fetchall()
bpa_times = np.asarray(temp, dtype = np.dtype([('episode', int), ('bpa_start_time', np.float64), ('bpa_end_time', np.float64)]))

bpa_start = np.zeros(len(inputs))
bpa_end = np.zeros(len(inputs))

controls = 0
cases = 0
control_inds = []
case_inds = []
for i in range(len(inputs)):
	ep = episodes[i]
	ind = np.where(bpa_times['episode'] == ep)[0]
	# For controls (which have no bpa time)
	if len(ind) == 0:
		bpa_start[i] = None
		bpa_end[i] = None
		controls = controls + 1
	# For cases: record bpa start and end times
	else:
		bpa_start[i] = bpa_times['bpa_start_time'][ind[0]]
		bpa_end[i] = bpa_times['bpa_end_time'][ind[0]]
		cases = cases + 1

# Creating labels, with option to move alert start to before actual alert
labels = []

offset = HOUR_OFFSET / 24.0 
for i in range(len(inputs)):
	# Initialize labels to zeros
	l = np.zeros(len(inputs[i]))
	# Update labels for cases to correspond to actual times of alert
	if ~np.isnan(bpa_start[i]):
		on_time = bpa_start[i] - offset
		off_time = bpa_end[i]
		times = inputs[i][:, 0]
		inds = np.where((times >= on_time) & (times <= off_time))[0]
		if len(inds) == 0:
			inds = -1
		l[inds] = 1
	labels.append(l)


# Randomly create training, testing and validation sets
seed = 89
np.random.seed(seed)

train_inds = random.sample(range(len(inputs)), int(round(len(inputs)*0.65)))
train_inputs = []
train_labels = []
train_starts = []
train_ends = []
for i in train_inds:
	train_inputs.append(inputs[i])
	train_labels.append(labels[i])
	train_starts.append(bpa_start[i])
	train_ends.append(bpa_end[i])

unused_inds = list(set(range(len(inputs))) - set(train_inds))
val_inds = random.sample(unused_inds, int(round(len(unused_inds)*0.25)))
val_inputs = []
val_labels = []
val_starts = []
val_ends = []
for i in val_inds:
	val_inputs.append(inputs[i])
	val_labels.append(labels[i])
	val_starts.append(bpa_start[i])
	val_ends.append(bpa_end[i])

test_inds = list(set(unused_inds) - set(val_inds))
test_inputs = []
test_labels = []
test_starts = []
test_ends = []
for i in test_inds:
	test_inputs.append(inputs[i])
	test_labels.append(labels[i])
	test_starts.append(bpa_start[i])
	test_ends.append(bpa_end[i])

# Creating model

seed = 543
np.random.seed(seed)

model = Sequential()
model.add(LSTM(128, input_dim = 1464, return_sequences = True, init = 'uniform'))
model.add(LSTM(128, init = 'uniform', return_sequences = True))
model.add(TimeDistributed(Dense(1, init = 'uniform', activation = 'sigmoid')))

model.compile(loss = 'binary_crossentropy', optimizer = 'adam')
json_string = model.to_json()

# Looping: epochs
# Looping: training examples
num_epochs = 50

training_loss = np.zeros(num_epochs)
validation_loss = np.zeros(num_epochs)

min_val_loss = 1

inds = range(len(train_inputs))

for epoch in range(num_epochs):
	print(epoch)
	# Training
	# Shuffle training examples each epoch
	np.random.shuffle(inds)
	for i in inds:
		#print(i)
		x = train_inputs[i]
		x = x[None, :, :]
		y = train_labels[i]
		y = y[None, :, None]
		if (~np.isnan(x).any()) and (~np.isnan(y).any()):
			t = model.train_on_batch(x, y)
	# Calculating training loss
	train_loss = 0
	num_examples = 0
	for i in range(len(train_inputs)):
		x = train_inputs[i]
		x = x[None, :, :]
		y = train_labels[i]
		y = y[None, :, None]
		if (~np.isnan(x).any()) and (~np.isnan(y).any()):
			num_examples = num_examples + 1
			train_predictions = model.predict(x, verbose = 0)
			train_loss = train_loss + log_loss(y.flatten(), train_predictions.flatten())
	training_loss[epoch] = train_loss / num_examples
	print(training_loss[epoch])
	val_loss = 0
	num_examples = 0
	for i in range(len(val_inputs)):
		x = val_inputs[i]
		x = x[None, :, :]
		y = val_labels[i]
		y = y[None, :, None]
		if (~np.isnan(x).any()) and (~np.isnan(y).any()):
			num_examples = num_examples + 1
			val_predictions = model.predict(x, verbose = 0)
			val_loss = val_loss + log_loss(y.flatten(), val_predictions.flatten())
	validation_loss[epoch] = val_loss / num_examples
	print(validation_loss[epoch])
	if validation_loss[epoch] < min_val_loss:
		print('New min val loss at epoch ' + str(epoch))
		min_val_loss = validation_loss[epoch]
		model.save_weights('model' + str(HOUR_OFFSET) + '_optimal_weights.hdf', overwrite = True)

# Evaluating
validation_predictions = []
for i in range(len(val_inputs)):
	x = val_inputs[i]
	x = x[None, :, :]
	#if ~np.isnan(x).any():
	pred = model.predict(x, verbose = 0)
	validation_predictions.append(pred[0])

# Just looking at any alert vs no alert 
case_labels = [max(i) for i in val_labels]
max_preds = [max(i)[0] for i in validation_predictions]
roc_auc_score(case_labels, max_preds)

# Looking at accuracy of alert (time between predicted and actual)
case_inds = [i for i in range(len(case_labels)) if case_labels[i] == 1]
control_inds = [i for i in range(len(case_labels)) if case_labels[i] == 0]
thresholds = np.arange(0, 1, 0.001)
time_diffs = np.zeros((len(case_inds) + 1, len(thresholds) + 1))
# How many controls have an alert at each threshold?
false_positives = np.zeros(len(thresholds))
# How many cases don't have an alert at this threshold?
false_negatives = np.zeros(len(thresholds))
# Summarizing the difference in time between model alert and actual alert
mean_timediff = np.zeros(len(thresholds))
median_timediff = np.zeros(len(thresholds))
for t in range(len(thresholds)):
	false_pos = 0
	for i in range(len(case_inds)):
		index = case_inds[i]
		if max(validation_predictions[index])[0] >= thresholds[t]:
			j = np.where(validation_predictions[index] >= thresholds[t])[0][0]
			first_on = val_inputs[index][j][0]
			actual_on = val_starts[index]
			time_diffs[i][t] = first_on - actual_on
		else:
			time_diffs[i][t] = None
	for i in range(len(control_inds)):
		index = control_inds[i]
		if max(validation_predictions[index])[0] >= thresholds[t]:
			false_pos = false_pos + 1
	false_negatives[t] = sum(np.isnan(time_diffs[:, t]))
	false_positives[t] = false_pos
	mean_timediff[t] = np.nanmean(time_diffs[:, t])
	median_timediff[t] = np.nanmedian(time_diffs[:, t])

import csv
fmain = open('model0_eval.csv', 'w')
fmain.write(','.join(('false_positives', 'false_negatives', 'mean_timediff', 'median_timediff\r\n')))
for t in range(len(thresholds)):
	print(t)
	fmain.write(','.join((str(false_positives[t]), str(false_negatives[t]), str(mean_timediff[t]), ''.join((str(median_timediff[t]), '\r\n')))))
fmain.close()
