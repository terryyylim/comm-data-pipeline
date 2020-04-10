resource "google_storage_bucket" "enrichment_beam" {
  name          = "d-gcp-enrichment-bucket"
  location      = "asia-southeast1"
  force_destroy = true
}

resource "google_storage_bucket_acl" "enrichment_beam_acl" {
  bucket = google_storage_bucket.enrichment_beam.name
  role_entity = [
    "OWNER:gcp-enrichment@gcp-enrichment.iam.gserviceaccount.com"
  ]
}

resource "google_storage_bucket_iam_binding" "enrichment_beam_binding" {
  bucket = google_storage_bucket.enrichment_beam.name
  role   = "roles/storage.objectViewer"
  members = [
    "serviceAccount:gcp-enrichment@gcp-enrichment.iam.gserviceaccount.com"
  ]
}