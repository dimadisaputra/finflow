# Dev environment — OCI Free Tier constraints
# Do NOT use distributed_mode = true on free tier.

region         = "ap-singapore-1"
compartment_id = "ocid1.compartment.oc1..REPLACE_ME"

# These match OCI free tier limits
distributed_mode = false
