import logging
from src import workflow


logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def lambda_handler(event, context):
    records = workflow()

    return {
        'statusCode': 200,
        'body': records
    }
