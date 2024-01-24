variable "credentials" {
    description = "My Credential Location"
    default = "~/.gcp/gcp.json"
}

variable "Project" {
    description = "My Project Name"
    default = "sharp-harbor-411301"
}

variable "location" {
    description = "My Project Location"
    default = "asia"
}

variable "bq_dataset_name" {
    description = "My BigQuery Dataset Name"
    default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "My Storage Bucket Name"
    default = "sharp-harbor-411301-bucket"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}