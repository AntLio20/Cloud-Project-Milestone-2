import redis

# Redis Connection Details
REDIS_HOST = "####"
REDIS_PORT = 6379
REDIS_PASSWORD = "sofe4630u"
IMAGE_KEY = "OntarioTech"

# Connect to Redis
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=0,
    password=REDIS_PASSWORD
)

# Retrieve the image from Redis
image_data = redis_client.get(IMAGE_KEY)

if image_data is None:
    print("Error: No image found in Redis.")
else:
    try:
        # Write data
        with open("received.jpg", "wb") as f:
            f.write(image_data)

        print("Image successfully received and saved as received.jpg.")

    except Exception as e:
        print(f"Error writing image file: {e}")
