variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "aws_profile" {
  description = "AWS SSO profile name (as in ~/.aws/config)"
  type        = string
  default     = "mco"
}

variable "aws_account_id" {
  description = "AWS account ID"
  type        = string
}

variable "project_name" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "mco-mesonet"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for photos and manifest"
  type        = string
  default     = "mco-mesonet"
}

variable "github_repo" {
  description = "GitHub repo in org/name format, used to scope the OIDC trust policy"
  type        = string
  default     = "mt-climate-office/mco-mesonet-photos"
}
