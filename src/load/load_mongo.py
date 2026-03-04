from pymongo import UpdateOne
from pymongo.errors import PyMongoError
import pymongo
from src.utils.logger import get_logger
logger = get_logger(__name__)

BATCH_SIZE = 10
def load_mongo(data):

    if not data:
        logger.info("No data to load")
        return

    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["policy"]
    collection = db["policyMaster"]

    collection.create_index(
        [("policyNo", 1), ("owningLocation", 1)],
        unique=True
    )

    try:
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i+BATCH_SIZE]

            operations = []

            for record in batch:
                operations.append(
                    UpdateOne(
                        {
                            "policyNo": record["policyNo"],
                            "owningLocation": record["owningLocation"]
                        },
                        {"$set": record},
                        upsert=True
                    )
                )

            result = collection.bulk_write(operations, ordered=False)

            logger.info(
                f"Batch {i//BATCH_SIZE + 1}: "
                f"Inserted={result.upserted_count}, "
                f"Updated={result.modified_count}"
            )

    except PyMongoError as e:
        logger.error(f"MongoDB error: {type(e).__name__}")
        raise

    finally:
        client.close()