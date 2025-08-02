import cloudinary
import cloudinary.uploader
from cloudinary.utils import cloudinary_url

# Cloudinary configuration
cloudinary.config( 
    cloud_name = "dbckzuq69", 
    api_key = "998686898358573", 
    api_secret = "A--Ad5mCa0TBCn-cAOUvq5VlSX0",  
    secure=True
)

def upload_default_image():
    """Upload the default 'no-image-available' image to Cloudinary and return its URL."""
    upload_result = cloudinary.uploader.upload(
        "no-image-available.webp",
        public_id="validator_icons/no-image-available",
        overwrite=True,
        format="png",
        fetch_format="auto",
        quality="auto",
        flags="sanitize"
    )

    optimized_url, _ = cloudinary_url(
        "validator_icons/no-image-available",
        width=360,
        height=360,
        crop="fill",
        fetch_format="auto",
        quality="auto"
    )

    return optimized_url

if __name__ == "__main__":
    default_image_url = upload_default_image()
    print(f"DEFAULT_IMAGE_URL = \"{default_image_url}\"")
    print("Copy the above line and paste it into your main script.")