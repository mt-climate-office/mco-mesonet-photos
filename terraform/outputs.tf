output "s3_bucket_name" {
  description = "S3 bucket for photos and manifest"
  value       = aws_s3_bucket.photos.bucket
}

output "cloudfront_domain" {
  description = "CloudFront distribution domain — use this as the base URL in the web app"
  value       = "https://${aws_cloudfront_distribution.photos.domain_name}"
}

output "cloudfront_distribution_id" {
  description = "CloudFront distribution ID — needed for cache invalidations"
  value       = aws_cloudfront_distribution.photos.id
}

output "acm_dns_validation_records" {
  description = "CNAME records to send to UMT IT for ACM certificate DNS validation"
  value = {
    for dvo in aws_acm_certificate.custom_domain.domain_validation_options : dvo.domain_name => {
      name  = dvo.resource_record_name
      type  = dvo.resource_record_type
      value = dvo.resource_record_value
    }
  }
}

output "github_actions_role_arn" {
  description = "IAM role ARN assumed by GitHub Actions via OIDC"
  value       = aws_iam_role.github_actions.arn
}
