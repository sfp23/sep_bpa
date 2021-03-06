import numpy as np
import MySQLdb
import os

os.chdir('/labs/shahlab/spoole/Sepsis/tensors/controls')
#os.chdir('/Users/spoole/Documents/Sepsis/tensors/cases')

con = MySQLdb.connect(host = 'ncbolabs-db1.stanford.edu', user = 'spoole', passwd = '***')
curs = con.cursor()

# Finding total num features
query = 'select feature from user_spoole.sepsis_feature_limits_labs;'
numrows = curs.execute(query)
lab_types = np.fromiter(curs.fetchall(), count = numrows, dtype = np.dtype([('description', np.str_, 50)]))

query = 'select feature from user_spoole.sepsis_feature_limits_vitals;'
numrows = curs.execute(query)
vital_types = np.fromiter(curs.fetchall(), count = numrows, dtype = np.dtype([('description', np.str_, 50)]))

features = np.hstack((vital_types['description'], lab_types['description'], ['b_' + i for i in lab_types['description']]))
n_features = len(features)

## Controls

query = 'select patient_id as pat_id, episode, start as episode_start, ' \
		'end as episode_end from user_spoole.sepsis_subset_controls;'
numrows = curs.execute(query)
episode_info = np.fromiter(curs.fetchall(), count = numrows, dtype = np.dtype([('pat_id', np.int), \
	('episode_num', np.int), ('episode_start', np.float64), ('episode_end', np.float64)]))


for i in range(len(episode_info)):

	print(i)

	pat_id = episode_info['pat_id'][i]
	episode_start = episode_info['episode_start'][i]
	episode_end = episode_info['episode_end'][i]
	episode_num = episode_info['episode_num'][i]

	# Labs
	query = 'select external_name, order_time, result_time, normalized_value from' \
			' (select *, (ord_num_value - min_val) / (max_val - min_val) as normalized_value from ' \
			' (select * from' \
			' (select * from (select lower(external_name) as external_name, age_at_ordering_date_in_days as order_time, age_at_result_time_in_days as result_time,' \
			' ord_num_value from stride6.lab_results_shc where patient_id = ' + str(pat_id) + \
			' and lower(external_name) in (select feature from user_spoole.sepsis_features)' \
			' and (age_at_ordering_date_in_days between ' + str(episode_start) + ' and ' + str(episode_end) + \
			' or age_at_result_time_in_days between ' + str(episode_start) + ' and ' + str(episode_end) + ')) as a) as b' \
			' left join' \
			' (select feature as external_name, max_val, min_val from user_spoole.sepsis_feature_limits_labs) as c' \
			' using(external_name)) as d' \
			' where ord_num_value <= max_val and ord_num_value >= min_val) as e order by result_time, order_time;'

	numrows = curs.execute(query)
	labs = np.fromiter(curs.fetchall(), count = numrows, dtype = np.dtype([('external_name', np.str_, 50), \
		('order_time', np.float64), ('result_time', np.float64), ('ord_num_value', np.float64)]))

	# Creating two labs arrays: order time (binary) and result time (with result)
	labs_ordered = np.array([(labs[j][1], 'b_' + labs[j][0], 1) for j in range(len(labs)) if (labs[j][1] >= episode_start) & (labs[j][1] <= episode_end) & (labs[j][1] < labs[j][2])], \
		dtype = np.dtype([('time', np.float64), ('description', np.str_, 50), ('value', np.float64)]))
	labs_result = np.array([(labs[j][2], labs[j][0], labs[j][3]) for j in range(len(labs)) if (labs[j][2] >= episode_start) & (labs[j][1] <= episode_end)], \
		dtype = np.dtype([('time', np.float64), ('description', np.str_, 50), ('value', np.float64)]))

	# Vitals
	query = 'select age_at_vital_recorded_in_days, detail_display, normalized_value from' \
			' (select *, (meas_value - min_val) / (max_val - min_val) as normalized_value from' \
			' (select * from' \
			' (select lower(detail_display) as detail_display, age_at_vital_recorded_in_days, meas_value from stride6.vitals where' \
			' patient_id = ' + str(pat_id) + ' and (age_at_vital_recorded_in_days between ' + str(episode_start) + ' and ' + str(episode_end) + ')) as a' \
			' left join' \
			' (select feature as detail_display, max_val, min_val from user_spoole.sepsis_feature_limits_vitals) as c using(detail_display)) as d' \
			' where meas_value <= max_val and meas_value >= min_val) as e order by age_at_vital_recorded_in_days;'

	numrows = curs.execute(query)
	vitals = np.fromiter(curs.fetchall(), count = numrows, dtype = np.dtype([('time', np.float64), ('description', np.str_, 50), ('value', np.float64)]))

	# Combining features and ordering
	data = np.hstack((vitals, labs_result, labs_ordered))

	timepoints = np.sort(np.unique(data['time']))
	n_timepoints = len(timepoints)

	formatted_data = np.zeros((n_timepoints, (2 + n_features)))
	# Time since start of episode
	formatted_data[:,0] = (timepoints - episode_start) / 100 # to scale to max value being ~1
	# Age at event 
	formatted_data[:,1] = timepoints / (365 * 90) # to scale to max value being ~1


	indicator_data = np.zeros((n_timepoints, (n_features)))

	for j in range(len(data)):
		row_ind = np.where(timepoints == data['time'][j])[0][0]
		col_ind = np.where(features == data['description'][j])[0][0] + 2
		formatted_data[row_ind:n_timepoints, col_ind] = data['value'][j]
		indicator_data[row_ind, (col_ind - 2)] = 1

	formatted_features = np.hstack((formatted_data, indicator_data))

	filename = str(pat_id) + '_' + str(episode_num) + '_features.npy'
	np.save(filename, formatted_features)

	# Alerts
	 	
	raw_outcome = np.zeros(n_timepoints)

	filename = str(pat_id) + '_' + str(episode_num) + '_outcome.npy'
	np.save(filename, raw_outcome)

	# offset = 3.0
	# offset_outcome = raw_outcome

	# if ((bpa_start - (offset / 24)) > max(timepoints)):
	# 	start_point_offset = len(timepoints) - 1
	# else:
	# 	start_point_offset = np.where((bpa_start - (offset / 24)) <= timepoints)[0][0]

	# if (found_end == 'Y') & (bpa_end < timepoints[-1]):
	# 	end_point_offset = np.where(bpa_end >= timepoints)[0][-1]
	# else:
	# 	end_point_offset = len(timepoints) - 1 

	# if end_point_offset < start_point:
	# 	end_point_offset = start_point

	# if end_point_offset <= start_point_offset:
	# 	offset_outcome[start_point] = 1
	# else:
	# 	offset_outcome[start_point_offset:end_point_offset] = 1

	# filename = str(episode_num) + '_outcome_offset_' + str(int(offset)) + '.npy'
	# np.save(filename, offset_outcome)


