# sep_bpa

BPA_firing: 
Finds times of alerts based on criteria in 2015_6_5 Sepsis BPA highlevel_specs.docx

cases_and_controls_save:
Match control episodes (with no alert) to case episodes based on episode duration and age at time of episode.

creating_tensors:
Extracts labs and vitals for each patient episode of interest and creates tensor (create both input and outcome arrays, but actually only use the input ones. Need to combine these into one file as there is no difference in processing for cases and controls when I only care about the input arrays). 

model1:
First attempt at building a neural net model. 
