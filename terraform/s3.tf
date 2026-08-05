resource "aws_s3_bucket" "photos" {
  bucket = var.s3_bucket_name
  tags   = local.common_tags
}

# Keep the bucket private — CloudFront OAC handles ALL public reads (the photo
# explorer distro and the data CDN, data2.climate.umt.edu/mesonet/*).
#
# History: from ~2026-05 to 2026-08-04 these flags were flipped off out-of-band
# to allow a public-read statement on air-quality/* (anonymous direct-S3
# access for the mesonet-aq archive's R consumers). Removed deliberately
# 2026-08-04 (Kyle: clean break): air-quality is served through the CDN at
# data2.climate.umt.edu/mesonet/air-quality/ like everything else, and direct
# unauthenticated S3-endpoint reads are no longer supported. If something
# out there still reads the raw endpoint, THIS is why it started 403ing.
resource "aws_s3_bucket_public_access_block" "photos" {
  bucket = aws_s3_bucket.photos.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Allow CloudFront OAC to read all objects, and GitHub Actions to read/write.
resource "aws_s3_bucket_policy" "photos" {
  bucket = aws_s3_bucket.photos.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudFrontRead"
        Effect = "Allow"
        Principal = {
          Service = "cloudfront.amazonaws.com"
        }
        Action   = "s3:GetObject"
        Resource = "${aws_s3_bucket.photos.arn}/*"
        Condition = {
          StringEquals = {
            # Two distributions read this bucket: the photo explorer (this
            # repo) and the MCO data CDN (mco-data-cdn repo — data2.climate.
            # umt.edu serves the whole bucket under /mesonet/*, incl. the
            # living Parquet archive in data/). The data CDN's ARN is
            # hardcoded because its state lives in the other repo.
            "AWS:SourceArn" = [
              aws_cloudfront_distribution.photos.arn,
              local.data_cdn_distribution_arn,
            ]
          }
        }
      },
      {
        # The data CDN's storage browser lists this bucket THROUGH the CDN
        # (https://data2.climate.umt.edu/mesonet/?list-type=2…): the bucket is
        # private, so the anonymous direct-S3 listing the browser uses for
        # public buckets 403s here — instead CloudFront's OAC signs the list
        # request, which needs this grant. The whole bucket is public-by-design
        # behind the CDN, so listing all keys exposes nothing new.
        Sid    = "DataCDNList"
        Effect = "Allow"
        Principal = {
          Service = "cloudfront.amazonaws.com"
        }
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.photos.arn
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = local.data_cdn_distribution_arn
          }
        }
      },
      {
        # Time travel for the living archive: its tag manifests record S3
        # versionIds, and as-of reads GET data/* with ?versionId=…. Scoped to
        # data/* ONLY — noncurrent versions of photos etc. stay unreachable.
        Sid    = "DataCDNVersionedRead"
        Effect = "Allow"
        Principal = {
          Service = "cloudfront.amazonaws.com"
        }
        Action   = "s3:GetObjectVersion"
        Resource = "${aws_s3_bucket.photos.arn}/data/*"
        Condition = {
          StringEquals = {
            "AWS:SourceArn" = local.data_cdn_distribution_arn
          }
        }
      },
      {
        Sid    = "GitHubActionsReadWrite"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.github_actions.arn
        }
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = "${aws_s3_bucket.photos.arn}/*"
      },
      {
        Sid    = "GitHubActionsListBucket"
        Effect = "Allow"
        Principal = {
          AWS = aws_iam_role.github_actions.arn
        }
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.photos.arn
      },
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.photos]
}

# CORS — needed for DuckDB-WASM to fetch manifest.parquet via range requests.
resource "aws_s3_bucket_cors_configuration" "photos" {
  bucket = aws_s3_bucket.photos.id

  cors_rule {
    allowed_headers = ["*"]
    allowed_methods = ["GET", "HEAD"]
    allowed_origins = ["*"]
    expose_headers  = ["Content-Length", "Content-Range", "Accept-Ranges", "ETag"]
    max_age_seconds = 3600
  }
}
