variable "project_id" {
  default = "sharp-harbor-411301"
}

variable "region" {
  default = "australia-southeast1"
}

variable "zone" {
  default = "australia-southeast1-b"
}

variable "credentials" {
    description = "My Credential Location"
    default = "~/.gcp/pg_mage_gcp.json"
}
