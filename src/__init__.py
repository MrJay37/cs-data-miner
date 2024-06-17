from boto3 import client
from boto3.session import Session
from datetime import time as t
from pytz import timezone, UTC
from src.interface import *


DOWNLOAD_DIR = '/tmp' if os.getenv('DOWNLOAD_DIR') is None else os.getenv('DOWNLOAD_DIR')

MARKET_TZ = 'America/New_York'
AWS_PROFILE = 'default' if os.getenv('AWS_PROFILE') is None else os.getenv('AWS_PROFILE')


def getEventRule(c):
    rules = c.list_rules()

    if 'Rules' not in rules.keys():
        raise Exception(f"Rules not found using boto3")

    rules = rules['Rules']

    try:
        rule = list(filter(lambda x: all([y in x['Name'] for y in ['charles', 'schwab']]), rules))[0]

    except IndexError:
        raise Exception(f"Charles Schwab trigger rule not found")

    return rule


def disableRule():
    s = Session(profile_name=AWS_PROFILE)

    c = s.client('events')

    rule = getEventRule(c)

    if rule['State'] == 'ENABLED':
        logging.warning(f"Disabling Charles Schwab data mining trigger rule")
        c.disable_rule(Name=rule['Name'])

    logging.info(f"Charles Schwab data mining trigger rule disabled")


def weeklyTokenRefresh():
    cs = createInterface()

    cs.weeklyRefresh()

    s = Session(profile_name=AWS_PROFILE)

    c = s.client('events')

    rule = getEventRule(c)

    if rule['State'] == 'DISABLED':
        logging.warning(f"Enabling Charles Schwab data mining trigger rule")
        c.enable_rule(Name=rule['Name'])

    logging.info(f"Charles Schwab data mining trigger rule enabled")


def reorganizeDataFiles():
    aws_session = Session(profile_name=AWS_PROFILE_NAME)

    s3 = aws_session.client('s3')

    bucket_name = os.getenv('S3_BUCKET_NAME')

    while True:
        res = s3.list_objects(Bucket=bucket_name, Prefix=f"datasets/CHAIN_")

        files = res['Contents']

        if len(files) == 0:
            break

        for fdx, file_obj in enumerate(files):
            key = file_obj['Key']

            file_name = key.split('/')[-1].split('.')[0]

            data_type, symbol, timestamp = file_name.split('_')

            obj_date, obj_time = timestamp[:8], timestamp[8:]

            # file_date_time = UTC.localize(dt.strptime(key.split('_')[-1].split('.')[0], '%Y%m%d%H%M%S%f'))

            new_key = f"datasets/{data_type}/{obj_date}/{symbol}_{obj_time}.json"

            logging.info(f"Moving {key} to {new_key}, size {file_obj['Size']}")

            s3.copy_object(Bucket=os.getenv('S3_BUCKET_NAME'), CopySource=f"{bucket_name}/{key}", Key=new_key)

            if key != new_key:
                s3.delete_object(Bucket=bucket_name, Key=key)


def workflow(on_hours=True, save=True) -> list:
    tz = timezone(TZ_NAME)

    now = tz.localize(dt.now())

    if on_hours and (
        now.astimezone(timezone(MARKET_TZ)).time() < t(9, 30) or
        # End time minutes = 33, because 4:30 PM API call must go through
        now.astimezone(timezone(MARKET_TZ)).time() > t(16, 33)
    ):
        logging.info(f'Running off hours [{now.astimezone(timezone(MARKET_TZ))}], skipping calls')
        return []

    a = createInterface()

    aws_session = Session(profile_name=AWS_PROFILE_NAME)

    s3 = aws_session.client('s3')

    quotes_list = os.getenv('QUOTES_LIST')

    if quotes_list is None:
        logging.error(f"Quotes not provided, nothing to call")
        return []

    quotes_list = [q.strip() for q in quotes_list.split(',')]

    records = []

    for q in quotes_list:
        as_of = tz.localize(dt.now())

        logging.info(f"Getting chain for {q} as of {as_of}")

        try:
            data = a.getChain(q)

        except ErrorCall as e:
            if e.body is not None:
                if 'errors' in e.body.keys():
                    for error in e.body['errors']:
                        logging.error(f"Get chain call failed for {q} [HTTP {e.status_code}]: {error['detail']}")

                else:
                    logging.error(
                        f"Get chain call failed for {q} [HTTP {e.status_code}]: {e.body['error_description']}"
                    )

                continue

            else:
                raise Exception(f"Call failed for {q} [HTTP {e.status_code}]: {e.reason}")

        file_name = f"CHAIN_{q}_{as_of.astimezone(UTC).strftime('%Y%m%d%H%M%S%f')}.json"

        file_path = DOWNLOAD_DIR + '/' + file_name

        if save:
            with open(file_path, 'w') as f:
                f.write(json.dumps(data))
                f.close()

            s3.upload_file(file_path, os.getenv('S3_BUCKET_NAME'), 'datasets/' + file_name)

            os.remove(file_path)

            records.append({
                'asOf': str(as_of),
                'symbol': q,
                'fileName': file_name
            })

    return records
