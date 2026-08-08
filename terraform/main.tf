terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Shared state bucket. Key is the REPO name (mco-mesonet-photos), not the
  # bucket name this stack manages (mco-mesonet) — the old commented key here
  # predated the org key convention. Auth comes from the environment
  # (AWS_PROFILE=mco locally, OIDC in CI).
  backend "s3" {
    bucket       = "mco-terraform-state"
    key          = "mco-mesonet-photos/terraform.tfstate"
    region       = "us-west-2"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}
