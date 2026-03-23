terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment once the shared state bucket exists.
  # backend "s3" {
  #   bucket  = "mco-terraform-state"
  #   key     = "mco-mesonet/terraform.tfstate"
  #   region  = "us-west-2"
  #   profile = "mco"
  # }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

# ACM certificates for CloudFront must be provisioned in us-east-1.
provider "aws" {
  alias   = "us_east_1"
  region  = "us-east-1"
  profile = var.aws_profile
}
