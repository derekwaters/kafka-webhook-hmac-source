import hmac
import hashlib

message = b"Hello from Authgear"
secret = b"mysecretkey"

signature = hmac.new(secret, message, hashlib.sha256).hexdigest()
print("Generated HMAC:", signature)

expected_signature = "your_received_signature_here"
is_valid = hmac.compare_digest(signature, expected_signature)
print("Signature valid?", is_valid)


---

import hashlib
import hmac
def verify_signature(payload_body, secret_token, signature_header):
    """Verify that the payload was sent from GitHub by validating SHA256.

    Raise and return 403 if not authorized.

    Args:
        payload_body: original request body to verify (request.body())
        secret_token: GitHub app webhook token (WEBHOOK_SECRET)
        signature_header: header received from GitHub (x-hub-signature-256)
    """
    if not signature_header:
        raise HTTPException(status_code=403, detail="x-hub-signature-256 header is missing!")
    hash_object = hmac.new(secret_token.encode('utf-8'), msg=payload_body, digestmod=hashlib.sha256)
    expected_signature = "sha256=" + hash_object.hexdigest()
    if not hmac.compare_digest(expected_signature, signature_header):
        raise HTTPException(status_code=403, detail="Request signatures didn't match!")