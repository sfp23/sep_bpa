# Finding BPA firing times


require(Rmpi)
require(doMPI)
require(foreach)

library(RMySQL)
library(chron)

# Connect to SQL database
# Note: password has been removed for security reasons, so this code will not run
drv = dbDriver("MySQL")
con = dbConnect(drv, user = "spoole", host = "ncbolabs-db1.stanford.edu", password = "*******")

# Find unique patients
unique_patients = unlist(dbGetQuery(con, 'SELECT * FROM user_spoole.sepsis_allSTRIDE_BPA_patients;'))

dbDisconnect(con)

# Unique 'M' categories
M_cats = as.factor(c('creatinine',  'fever', 'tachycardia', 'hyperlactatemia', 'hypotension_MAP', 'hypotension_SBP', 'hypothermia', 'leukocytosis', 'leukopenia', 'tachypnea', 'thrombocytopeni'))
S_cats = as.factor(c('bloodcult', 'lactate'))
OD_cats = as.factor(c('hyperlactatemia', 'hypotension_MAP', 'hypotension_SBP'))

# BPA table 
#bpa_table = data.frame('patient_id' = NA, 'start' = NA, 'end' = NA) 

# Initialize file 
#write.table(t(c(colnames(bpa_table), 'Ended?')), file = 'bpa_table.csv', sep = ',', row.names = FALSE, col.names = FALSE)

find_BPA_times = function(pat_num) {
  
  library(RMySQL)
  library(chron)
  
  # Connect to SQL database
  # Note: password has been removed for security reasons, so this code will not run
  drv = dbDriver("MySQL")
  con_int = dbConnect(drv, user = "spoole", host = "ncbolabs-db1.stanford.edu", password = "7X_QLudcFx7e")
  
  print(pat_num)
  
  pat = unique_patients[pat_num]
  
  print(pat)
  
  M_flags = rep(0, length(M_cats))
  M_start = rep(NA, length(M_cats))
  M_end = rep(NA, length(M_cats))
  S_flags = rep(0, length(S_cats))
  S_start = rep(NA, length(S_cats))
  S_end = rep(NA, length(S_cats))
  OD_flags = rep(0, length(OD_cats))
  OD_start = rep(NA, length(OD_cats))
  OD_end = rep(NA, length(OD_cats))
  
  overall_flag = c(0, 0, 0, 0)
  overall_start = NA
  overall_end = c(NA, NA, NA) # M, S, OD

  query = paste('SELECT * FROM (SELECT code, cat, lookback, age_at_result_time_in_days AS datetime FROM user_spoole.sepsis_M_creatinine WHERE patient_id = ', pat, 
  ' UNION SELECT code, cat, lookback, age_at_vital_entered_in_days AS datetime FROM user_spoole.sepsis_M_fever WHERE patient_id = ', pat,
  ' UNION SELECT code, cat, lookback, age_at_result_time_in_days AS datetime FROM user_spoole.sepsis_M_hyperlactatemia WHERE patient_id = ', pat,
  ' UNION SELECT code, cat, lookback, age_at_vital_entered_in_days AS datetime FROM user_spoole.sepsis_M_hypotension_MAP WHERE patient_id = ', pat,
  ' UNION SELECT code, cat, lookback, age_at_vital_entered_in_days AS datetime FROM user_spoole.sepsis_M_hypotension_SBP WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_vital_entered_in_days AS datetime FROM user_spoole.sepsis_M_hypothermia WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_result_time_in_days AS datetime FROM user_spoole.sepsis_M_leukocytosis WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_result_time_in_days AS datetime FROM user_spoole.sepsis_M_leukopenia WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_vital_entered_in_days AS datetime FROM user_spoole.sepsis_M_tachycardia WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_vital_entered_in_days AS datetime FROM user_spoole.sepsis_M_tachypnea WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_result_time_in_days AS datetime FROM user_spoole.sepsis_M_thrombocytopenia WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_result_time_in_days AS datetime FROM user_spoole.sepsis_OD_hyperlactatemia WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_vital_entered_in_days AS datetime FROM user_spoole.sepsis_OD_hypotension_MAP WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_vital_entered_in_days AS datetime FROM user_spoole.sepsis_OD_hypotension_SBP WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_ordering_date_in_days AS datetime FROM user_spoole.sepsis_S_bloodcult WHERE patient_id = ', pat,
	' UNION SELECT code, cat, lookback, age_at_ordering_date_in_days AS datetime FROM user_spoole.sepsis_S_lactate WHERE patient_id = ', pat,
	') AS sub_table;', sep = '')
  
  data = dbGetQuery(con_int, query)
  data = data[order(data$datetime), ]
  data$lookback = data$lookback / 24
  
  print('data obtained')
  
  dbDisconnect(con_int)
  lapply(dbListConnections(dbDriver( drv = "MySQL")), dbDisconnect)
  
  print('disconnected')
  
  bpa_firings = c(pat, NA, NA, NA)
  
  bpas_found = 0
  
  for (row_num in 1:nrow(data)) {
    
 # for (row_num in 1:35) {
    
    # Select data
    row = data[row_num, ]
    
    # ----------------------------------------------------------------------------------------
    ## Reset flags
    
    # M
    M_order = order(M_end, decreasing = FALSE)
    # Run through M flags, starting at the one which ends first
    for (i in M_order) {
      # Stop when no more flags are on
      if (M_flags[i] == 0) {
        break
      }
      # Check whether current date is past the end of each flag time period
      if (row$datetime > M_end[i]) {
        # Turn off flag and reset start value
        M_flags[i] = 0
        M_start[i] = NA
        # If M category falls below threshold, turn off overall M flag and add end time
        if (sum(M_flags) < 3) {
          if (overall_flag[1] == 1) {
            overall_end[1] = M_end[i]
            overall_flag[1] = 0
          }
        }
        M_end[i] = NA
      }
    }
    
    # S
    S_order = order(S_end, decreasing = FALSE)
    # Run through S flags, starting at the one which ends first
    for (i in S_order) {
      # Stop when no more flags are on
      if (S_flags[i] == 0) {
        break
      }
      # Check whether current date is past the end of each flag time period
      if (row$datetime > S_end[i]) {
        # Turn off flag and reset start value
        S_flags[i] = 0
        S_start[i] = NA
        # If M category falls below threshold, turn off overall M flag and add end time
        if (sum(S_flags) < 1) {
          if (overall_flag[2] == 1) {
            overall_end[2] = S_end[i]
            overall_flag[2] = 0
          }
        }
        S_end[i] = NA
      }
    }
    
    # OD
    OD_order = order(OD_end, decreasing = FALSE)
    # Run through OD flags, starting at the one which ends first
    for (i in OD_order) {
      # Stop when no more flags are on
      if (OD_flags[i] == 0) {
        break
      }
      # Check whether current date is past the end of each flag time period
      if (row$datetime > OD_end[i]) {
        # Turn off flag and reset start value
        OD_flags[i] = 0
        OD_start[i] = NA
        # If M category falls below threshold, turn off overall M flag and add end time
        if (sum(OD_flags) < 1) {
          if (overall_flag[3] == 1) {
            overall_end[3] = OD_end[i]
            overall_flag[3] = 0
          }
        }
        OD_end[i] = NA
      }
    }
    
    # Check if BPA is now turned off
    if ((overall_flag[4] == 1) && (sum(overall_flag[1:3]) < 3)) {
      # Record alert with start and end times
      new_bpa = c(pat, overall_start, min(overall_end, na.rm = TRUE), 'Y')
      #write.table(t(new_bpa), file = 'bpa_table.csv', sep = ',', row.names = FALSE, col.names = FALSE, append = TRUE)
      #rbind(bpa_table, new_bpa)
      bpa_firings = rbind(bpa_firings, new_bpa)
      bpas_found = bpas_found + 1
      overall_flag[4] = 0
      overall_start = NA
      overall_end = c(NA, NA, NA)
    }
    
    
    # ----------------------------------------------------------------------------------------
    
    # Turn on appropriate flag(s) according to data in this row
    
    if (row$code == 'M') {
      cat_num = which(M_cats == row$cat)
      M_flags[cat_num] = 1
      M_end[cat_num] = row$datetime + row$lookback
      # Only update start time if flag is not already on
      if (is.na(M_start[cat_num])) {
        M_start[cat_num] = row$datetime
      }
      if (sum(M_flags) >= 3) {
        overall_flag[1] = 1
      }
    } else if (row$code == 'S') {
      cat_num = which(S_cats == row$cat)
      S_flags[cat_num] = 1
      S_end[cat_num] = row$datetime + row$lookback
      # Only update start time if flag is not already on
      if (is.na(S_start[cat_num])) {
        S_start[cat_num] = row$datetime
      }
      if (sum(S_flags) >= 1) {
        overall_flag[2] = 1
      }
    } else {
      cat_num = which(OD_cats == row$cat)
      OD_flags[cat_num] = 1
      OD_end[cat_num] = row$datetime + row$lookback
      # Only update start time if flag is not already on
      if (is.na(OD_start[cat_num])) {
        OD_start[cat_num] = row$datetime
      }
      if (sum(OD_flags) >= 1) {
        overall_flag[3] = 1
      }
    } 
    
    if ((sum(overall_flag[1:3]) == 3) && overall_flag[4] == 0) {
      overall_flag[4] = 1
      overall_start = row$datetime
      overall_end = c(NA, NA, NA)
    }   
  }

  # Check if flag is still on, waiting for endpoint
  if (overall_flag[4] == 1) {
    # Record alert with start time and max time seen 
    new_bpa = c(pat, overall_start, max(data$datetime), 'N')
    #write.table(t(new_bpa), file = 'bpa_table.csv', sep = ',', row.names = FALSE, col.names = FALSE, append = TRUE)
    #rbind(bpa_table, new_bpa)
    bpa_firings = rbind(bpa_firings, new_bpa)
    bpas_found = bpas_found + 1
    overall_flag[4] = 0
    overall_start = NA
    overall_end = c(NA, NA, NA)
  }
  
  if (bpas_found > 0) {
    bpa_firings = bpa_firings[-(which(is.na(bpa_firings[, 2]))), ]
    return(bpa_firings)
  }
}


cl = startMPIcluster()
registerDoMPI(cl)

N = length(unique_patients)

delta = 130000

start = 1
end = delta - 1

while (start < N) {
  print(start)
  if (end > N) {
    end = N
  }
  print(end)
  hold_results = foreach(i = start:end, .combine = 'rbind') %dopar% find_BPA_times(i)
  string = paste('bpa_firing_', start, '_', end, '.Rdata', sep = '')
  print(string)
  save(hold_results, file = string)
  start = end + 1
  end = end + delta
  print('round_complete')
}

#hold_results = foreach(i = 1:100, .combine = 'rbind') %dopar% find_BPA_times(i)

#sapply(1:length(unique_patients), find_BPA_times)

closeCluster(cl)
mpi.finalize()

#save(list = ls(all = TRUE), file = 'bpa_firing_all.Rdata')





















