provider "google" {
  version = "3.5.0"

  credentials = file("~/.secrets/gcp-enrichment-auth.json")

  project = var.project_name
}