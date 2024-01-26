variable "credentials" {
  description = "My Credentials"
  default     = "~/.gcp/gcp.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}


variable "project" {
  description = "Project"
  default     = "sharp-harbor-411301"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "australia-southeast1-a"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "ASIA"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  #Update the below to what you want your dataset to be called
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  #Update the below to a unique bucket name
  default     = "sharp-harbor-411301-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}