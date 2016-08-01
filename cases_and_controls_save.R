
library(RMySQL)
library(chron)

# Episode controls
# Connect to SQL database
# Note: password has been removed for security reasons, so this code will not run
drv = dbDriver("MySQL")
con = dbConnect(drv, user = "spoole", host = "ncbolabs-db1.stanford.edu", password = "***")

cases = unlist(dbGetQuery(con, 'select distinct episode from user_spoole.sepsis_allSTRIDE_episodes union select distinct episode from user_spoole.sepsis_randomized_episodes;'))
all_data = dbGetQuery(con, 'select * from user_spoole.sepsis_episodes_with_counts where (lab_count > 0 or vitals_count > 0)')

dbDisconnect(con)

controls_episodes = all_data[-(which(all_data$episode %in% cases)), ] 

drv = dbDriver("MySQL")
con = dbConnect(drv, user = "spoole", host = "ncbolabs-db1.stanford.edu", password = "***")
cases = dbGetQuery(con, 'select * from user_spoole.sepsis_subset_cases')
dbDisconnect(con)

allcontrols = controls_episodes

library(FNN)
# Matching on age at episode and duration of episode (i.e. length of stay)
output = get.knnx(data = allcontrols[, c(5, 3)], query = cases[, c(10, 8)], k = 1000)

# Indices of cases that have exact matches, sorted by how many exact matches they have
exact_inds = as.numeric(names(sort(table(which(output$nn.dist == 0, arr.ind = TRUE)), desc = TRUE)))

cases$matched = FALSE

count_nomatch = 0
max_match = 0

# Match exact matches first 
for (i in exact_inds) {
  j = 1
  while (cases$matched[i] == FALSE) {
    if (output$nn.index[i, j] %in% cases$matched) {
      j = j + 1
      if (j > 1000) {
        count_nomatch = count_nomatch + 1
        break
      }
    } else {
      cases$matched[i] = output$nn.index[i, j]
      if (j > max_match) {
        max_match = j
      }
    }
  }
}

# Matching other examples
not_exact = setdiff(1:nrow(cases), exact_inds)

for (i in not_exact) {
  j = 1
  while (cases$matched[i] == FALSE) {
    if (output$nn.index[i, j] %in% cases$matched) {
      j = j + 1
      if (j > 1000) {
        count_nomatch = count_nomatch + 1
        break
      }
    } else {
      cases$matched[i] = output$nn.index[i, j]
      if (j > max_match) {
        max_match = j
      }
    }
  }
}

matched_controls = allcontrols[cases$matched, ]

write.csv(matched_controls, file = 'matched_controls.csv', row.names = FALSE)

