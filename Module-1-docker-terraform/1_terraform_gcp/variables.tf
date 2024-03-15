variable "credentials" {
  description = "My Credentials"
  default     = "/home/peter/.gc/ny-rides-peter.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}

variable "project" {
  description = "Your GCP Project ID"
  default     = "ny-rides-peter-415106"
  type        = string
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default = "asia-southeast1"
}

variable "zone" {
  description = "Zone"
  #Update the below to your desired region
  default = "asia-southeast1-b"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default = "asia-southeast1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default = "homework_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  # Update the below to a unique bucket name
  default = "homework_bucket_415106"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
