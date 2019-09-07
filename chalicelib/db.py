import boto3


DEFAULT_USERNAME = "default"


class DynamoDBUser:
    def __init__(self):
        self._table = boto3.resource("dynamodb").Table("Users")

    def list_all_users(self):
        response = self._table.scan()
        return response["Items"]

    def add_user(self, mac_address, username=DEFAULT_USERNAME):
        self._table.put_item(
            Item={
                "username": username,
                "mac_address": mac_address,
                "time_stamp": "Never",
                "is_online": False,
            }
        )
        return username

    def delete_user(self, username=DEFAULT_USERNAME):
        self._table.delete_item(Key={"username": username})

    def update_user(self, mac_address=None, username=DEFAULT_USERNAME):
        user = self._table.get_item(Key={"username": username})["Item"]
        if mac_address is not None:
            user["mac_address"] = mac_address
        self._table.put_item(Item=user)

    def update_user_status(self, mac_address=None, time_stamp=None, is_online=False):
        user = self._table.get_item(Key={"mac_address": mac_address})["Item"]
        if not user:
            return
        if time_stamp is not None:
            user["time_stamp"] = time_stamp
        if is_online is not None:
            user["is_online"] = is_online
        self._table.put_item(Item=user)

    def add_bulk_records(self, records):
        self._table.batch_write_item(RequestItems=records)

    def fetch_report_data(self):
        # TODO implement a complex aggregation to get the data for email
        return self._table.get_items()["Item"]
