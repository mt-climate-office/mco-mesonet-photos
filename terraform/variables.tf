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
  default     = "mt-climate-office/mesonet-photos"
}

variable "github_repo_aliases" {
  description = <<-EOT
    Additional subject identities the OIDC trust accepts, each rendered as
    `repo:<value>:*`. Two distinct uses:

    1. THE IMMUTABLE SUBJECT (permanent, load-bearing). Renaming this repo on
       2026-08-05 made GitHub switch it from the plain `org/name` subject claim
       to an ID-embedded one — CloudTrail showed the token go from
       `repo:mt-climate-office/mco-mesonet-photos:...` (worked) to
       `repo:mt-climate-office@35075063/mesonet-photos@1187521045:...` (denied)
       across the rename. Those numeric org/repo IDs never change, so trusting
       that form is what actually makes this role survive future renames. Do not
       remove it. Read the live value with:
         gh api repos/mt-climate-office/mesonet-photos/actions/oidc/customization/sub

    2. Rename transitions (temporary). A plain `org/name` entry, kept only until
       a rename is verified. Drop it afterwards: any name left here could be
       claimed by a new repo in this org, which would then be able to write to
       the photos bucket.
  EOT
  type        = list(string)
  default     = []
}
