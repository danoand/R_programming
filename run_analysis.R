print("TIDY: Begin processing.")

# Require the 'plyr' package
print("Loading required package: 'plyr'.")
require("plyr")

# Create working variables
in_scope_col_num <- numeric()            # Vector to hold the feature number
in_scope_col_name <- character()         # Vector to hold the feature name
out_scope_col_num <- numeric()           # Vector to hold the feature numbers that are to be removed
subject_col_name <- "SubjectId"
activity_col_name <- "Activity"
work_folder <- "tidy_work"
output_folder <- "tidy_output"
output_tidy_dataset <- "HumanActivitySmartphonesDataSet_TidyData.txt"
output_tidy_average_dataset <- "HumanActivitySmartphonesDataSet_Average_TidyData.txt"

################################################################
# FUNCTION: Create two directories
#             1. "tidy_work":    will contain interim files written to disk
#             2. "tidy_output":  will contain output of the tidy operation
#
create_tidy_folders <- function() {
    dir.create("tidy_work", showWarnings = FALSE)
    dir.create("tidy_work/test", showWarnings = FALSE)
    dir.create("tidy_work/train", showWarnings = FALSE)
    dir.create("tidy_output", showWarnings = FALSE)
    
    print("TIDY: Created working directories.")
}

################################################################
# FUNCTION: Add activity and subject columns
#
# RETURN: Filename of the R object written to disk
#
add_subject_activity_data <- function(in_file_subject, in_file_activity, in_file_observations) {
  # Read the subject data into a vector
  tmp_process_subject <- scan(in_file_subject)
  tmp_process_subject <- as.numeric(tmp_process_subject)
  
  # Read the activity data into a vector
  tmp_process_activity <- scan(in_file_activity)
  tmp_process_activity <- as.numeric(tmp_process_activity)
  
  # Read observation data into a data frame
  tmp_process_df_observations <- read.table(in_file_observations)
  
  # Bind the subject & activity columns to the observation data frame
  tmp_process_df_resultset <- cbind(tmp_process_df_observations, tmp_process_subject, tmp_process_activity)
  
  # Create a file name to use to write out the result to disk (as an R object)
  tmp_process_filename <- paste("tidy_work/", in_file_observations, ".rds", sep = "")
  
  # Save the result set data frame to disk as an R object
  saveRDS(tmp_process_df_resultset, file = tmp_process_filename)
  
  print("TIDY: Read observation data and assigned subject and activity codes.")
  
  # Return the path/filename to the calling statement
  tmp_process_filename
}

################################################################
# FUNCTION: Convert Activity Factors to Textual Descriptions
#
# RETURN: Filename of the R object written to disk
#
convert_activity_codes_to_text <- function(in_r_object) {
  # Read in activity description data
  tmp_convert_code_data <- read.table("activity_labels.txt")
  
  # Subset the code descriptions into a vector
  tmp_convert_code_descriptions <- tmp_convert_code_data$V2
  

  
  # Read in the R object from disk - processed observational data
  tmp_working_dataframe <- readRDS(in_r_object)
  
  # Subset the processed data to extract the activity code column as a vector
  tmp_working_vector_activity_codes <- tmp_working_dataframe[,ncol(tmp_working_dataframe)]
  
  # Iterate through the vector of activity codes (e.g. 1) and produce a vector of 
  #    the associated activity descriptions (e.g. WALKING)
  for (i in 1:length(tmp_working_vector_activity_codes)) {
    # If iterating the first time, create a new vector to contain the activity descriptions
    if (i == 1) {
      
      # Create the activity description vector
      tmp_working_vector_activity_descriptions <- character()
      # Push the activity description onto the vector associated with the activity code
      tmp_working_vector_activity_descriptions <- c(tmp_working_vector_activity_descriptions, as.character(tmp_convert_code_descriptions[tmp_working_vector_activity_codes[i]]))
      
      } else {
        
        # Push the activity description onto the vector associated with the activity code
        tmp_working_vector_activity_descriptions <- c(tmp_working_vector_activity_descriptions, as.character(tmp_convert_code_descriptions[tmp_working_vector_activity_codes[i]]))
        
      }
    }
    
    # Produced a vector of activity codes...
    # Remove the activity code column from the data frame and bind the activity description data
    tmp_working_dataframe <- tmp_working_dataframe[, -ncol(tmp_working_dataframe)]
    
    # Bind the new activity decription column
    tmp_working_dataframe <- cbind(tmp_working_dataframe, tmp_working_vector_activity_descriptions)
    
    # Write the data frame to disk as an R object - overwriting the inbound R object
    saveRDS(tmp_working_dataframe, file = in_r_object)
    
    print("TIDY: Translated Activity codes to textual descriptions.")
    
    # Return the object filename
    in_r_object
}

################################################################
# FUNCTION: Merge datasets into an R object that is saved to 
#             disk for processing downstream
#
# RETURN: Filename of the R object written to disk
#
merge_data_sets <- function(in_vector_r_objects) {
  # Create working variables
  tmp_merge_list <- list()
  
  # Iterate through the r-objects and read each into a variable
  for (i in 1:length(in_vector_r_objects)) {
    # Read the r object from disk to memory (R environment variable)
    tmp_interim_object <- readRDS(in_vector_r_objects[i])
    
    # Add the object to the list
    tmp_merge_list[[i]] <- tmp_interim_object
  }
  
  # Iterate through the list and combine into one object
  for (i in 1:length(tmp_merge_list)) {
    # If the first iteration, create an object to be appended to
    if (i == 1) {
        
      tmp_build_merge_object <- tmp_merge_list[[i]]
      
      } else {
          
          tmp_build_merge_object <- rbind(tmp_build_merge_object, tmp_merge_list[[i]])
        
      }
  }
  
  # Write the merged data set to disk
  tmp_merge_object_disk <- "tidy_work/merged_data_sets.rds"
  saveRDS(tmp_build_merge_object, file = tmp_merge_object_disk)
  
  # Remove R objects
  remove(tmp_build_merge_object, tmp_merge_list, tmp_interim_object)
  
  print("TIDY: Merged test and training datasets.")
  
  # Return the merged object filename
  tmp_merge_object_disk
}

################################################################
# FUNCTION: Identify feature list
# 
# Identify the features to be fetched from the data sets
#
identify_feature_list <- function(in_file) {
  # Read the file containing the feature information, which includes:
  #   1. Number of the specified column in the dataset
  #   2. Name of the specified column in the dataset
  df_col_info <- read.table(in_file)
  
  id_in_scope_col_num <- numeric()            # Vector to hold the feature number
  id_in_scope_col_name <- character()         # Vector to hold the feature name
  id_out_scope_col_num <- numeric()           # Vector to hold the feature numbers that are to be removed
  
  # Create a function used when iterating through the feature dataframe
  # The function effectively removes references to features other than mean() and std()
  scope_dataframe <- function(x) {
          # Test if the colname includes 'mean()' or 'std()'
          if ( grepl("mean()", x[2], fixed=TRUE) | grepl("std()", x[2], fixed=TRUE) ) {
                  # Add column number and name to their respective temporary vectors representing in scope features
                  id_in_scope_col_num <<- c(id_in_scope_col_num, as.numeric(x[1]))
                  id_in_scope_col_name <<- c(id_in_scope_col_name, x[2])
                  } else {
                          # Add the colunm number to the vector representing out of scope features
                          id_out_scope_col_num <<- c(id_out_scope_col_num, as.numeric(x[1]))
                  }
  }
  
  # Iterate through the feature list using the apply function calling: scope_dataframe
  outapply <- apply(df_col_info, 1, scope_dataframe)
  
  # Add two more columns to represent the subject & activity data
  id_in_scope_col_num <- c(id_in_scope_col_num, length(df_col_info) + 1, length(df_col_info) + 2) # Column number
  id_in_scope_col_name <- c(id_in_scope_col_name, subject_col_name, activity_col_name)
  
  # Pass back to the main program by assigning to global variables:
  #   1. The column numbers identified for processing
  #   2. The column names identified for processing
  #   3. The column numbers to be excluded from processing
  in_scope_col_num <<- id_in_scope_col_num
  in_scope_col_name <<- id_in_scope_col_name
  out_scope_col_num <<- id_out_scope_col_num
  
  print("TIDY: Identify unwanted observations and add Activity descriptions as a data column.")
  
  # Clean up by removing large objects from the environment
  remove(df_col_info, outapply)
}

################################################################
# FUNCTION: Process a data set (test or train) and output a data 
#           frame object to disk
#
process_dataset <- function(in_r_object) {
  # Create the scoped data frame
  updated_feature_list <- data.frame(in_scope_col_num, in_scope_col_name)
  colnames(updated_feature_list) <- c("ColumnNumber", "ColumnName")
  # We now have an updated feature list

  # Determine the number of columns we're scoping down to
  number_columns <- nrow(updated_feature_list)
  
  # Read in the dataset from an R object on disk
  tmp_process_data_object <- readRDS(in_r_object)
  
  # Remove columns not needed or out of scope (non 'mean' and non 'std' columns)
  tmp_process_data_object <- tmp_process_data_object[ , -out_scope_col_num]
  
  # Assign official column names to the test data
  colnames(tmp_process_data_object) <- in_scope_col_name
  
  # Create a filename for the R object to be written to disk
  tmp_process_data_out_filename <- "tidy_work/processed_data_set.rds"

  # Write test data object to disk for processing later
  saveRDS(tmp_process_data_object, file = tmp_process_data_out_filename)
  
  # Clean up by removing large objects from the environment
  remove(tmp_process_data_object)
  
  print("TIDY: Remove extraneous observations.")
  
  # Return the name of the output file (R object on disk)
  tmp_process_data_out_filename
}

################################################################
# FUNCTION: Produce a tidy data set and write to the "tidy_output" 
#           folder
#
produce_tidy_dataset <- function(in_r_object, in_filename) {
    # Read the R object from disk into an R object in memory
    tmp_produce_tidy_dataframe <- readRDS(in_r_object)
    
    # Remove the subject and activity columns
    tmp_number_columns <- ncol(tmp_produce_tidy_dataframe)

    tmp_produce_tidy_dataframe <- tmp_produce_tidy_dataframe[c((tmp_number_columns-1),tmp_number_columns,1:(tmp_number_columns-2))]

    # Write out the tidy dataset
    write.table(tmp_produce_tidy_dataframe, file = paste("tidy_output/", output_tidy_dataset, sep=""), row.names = FALSE)
    
    # Save the tidy dataset as a R object
    tmp_r_object_tidy_name = paste("tidy_work/", output_tidy_dataset, ".rds", sep="")
    saveRDS(tmp_produce_tidy_dataframe, file=tmp_r_object_tidy_name)
    
    print("TIDY: Generate a tidy data file representing the core data.")
    
    # Return the R object filename
    tmp_r_object_tidy_name
}

produce_tidy_dataset_average <- function(in_r_object, in_filename) {
  # Read the R object from disk into an R object in memory
  tmp_produce_tidy_interim_dataframe <- readRDS(in_r_object)
  
  # Use the plyr function to calculate column means by the 'SubjectId' and 'Activity' factors
  tmp_produce_tidy_average_dataframe <- ddply(tmp_produce_tidy_interim_dataframe, c('SubjectId', 'Activity'), function(x) colMeans(x[3:ncol(x)]))
  
  # Get the column names
  tmp_colnames <- colnames(tmp_produce_tidy_average_dataframe)
  
  # Update the column names to reflect an average (or mean) calculation 
  for (i in 3:ncol(tmp_produce_tidy_average_dataframe)) {
    tmp_colnames[i] <- paste("Avg:", tmp_colnames[i], sep=" ")
  }
  
  # Set updated column names
  colnames(tmp_produce_tidy_average_dataframe) <- tmp_colnames
  
  # Write the dataframe to the output folder
  write.table(tmp_produce_tidy_average_dataframe, paste(output_folder, output_tidy_average_dataset, sep="/"), row.names = FALSE)
  
  # Save the dataframe to disk as an R object
  tmp_r_object_dataset <- paste(output_tidy_average_dataset, ".rds", sep="")
  saveRDS(tmp_produce_tidy_average_dataframe, paste(work_folder, tmp_r_object_dataset, sep="/"))
  
  print("TIDY: Generate a tidy data file representing the average or mean of each core data variable.")
  
  # Return the r object filename
  tmp_r_object_dataset
}

################################################################
# MAIN PROGRAM: Main logic to set up processing and call the 
#                 functions that conduct the bulk of the work
#

# Create folders used by the tidy operation
create_tidy_folders()

# Read observation data, add subject & activity data, and write to an R object on disk
test_R_object <- add_subject_activity_data("test/subject_test.txt", "test/y_test.txt", "test/X_test.txt")
train_R_object <- add_subject_activity_data("train/subject_train.txt", "train/y_train.txt", "train/X_train.txt")

# Convert Activity Codes to Textual Descriptions
test_R_object <- convert_activity_codes_to_text(test_R_object)
train_R_object <- convert_activity_codes_to_text(train_R_object)

# Merge the data sets and write the merged data structure to disk
merged_datasets_R_object <- merge_data_sets(c(train_R_object, test_R_object))

# Call the 'identify_feature_list' function to identify the features we are processing
identify_feature_list("features.txt")

# Process data sets
processed_dataset_R_object <- process_dataset(merged_datasets_R_object)

# Produce tidy dataset
out_tidy_dataset <- produce_tidy_dataset(processed_dataset_R_object)

# Produce the tidy observation average dataset
produce_tidy_dataset_average(out_tidy_dataset, output_tidy_average_dataset)

print("TIDY: End processing.")
