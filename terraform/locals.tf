locals {
  common_tags = {
    Project   = var.project_name
    ManagedBy = "terraform"
  }

  # Built rather than referenced: this distribution belongs to the mco-data-cdn
  # repo, whose state we cannot read from here.
  data_cdn_distribution_arn = "arn:aws:cloudfront::${var.aws_account_id}:distribution/${var.data_cdn_distribution_id}"
}
