variable "bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
  default     = "ecommerce-raw-data-deepak-2024"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "ecommerce-pipeline"
}