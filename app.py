from chalice import Chalice, Rate, WebsocketDisconnectedError
from chalicelib import db, email
import boto3

s3 = boto3.resource("s3")
app = Chalice(app_name="big-brother")
app.debug = True
_DB = None

app.websocket_api.session = boto3.session.Session()
app.experimental_feature_flags.update(["WEBSOCKETS"])


def get_db():
    global _DB
    if _DB is None:
        _DB = db.DynamoDBUser()
    return _DB


@app.route("/users")
def users():
    return get_db().list_all_users()


@app.route("/users", method=["POST"])
def add_user():
    user_details = app.current_request.json_body
    return get_db().add_user(**user_details)


@app.route("/users/{username}", method=["PATCH"])
def update_user(username):
    user_details = app.current_request.json_body
    return get_db().update_user(**user_details)


@app.route("/users/{username}", method=["DELETE"])
def delete_user(username):
    return get_db().delete_user(username)


@app.route("/data", method=["POST"])
def data():
    dump = app.current_request.json_body
    db_ref = get_db()
    for record in dump:
        db_ref.update_user_status(**record)
    return {"success": True}


@app.on_ws_message()
def message(event):
    try:
        dump = event.body
        db_ref = get_db()
        for record in dump:
            db_ref.update_user_status(**record)

    except WebsocketDisconnectedError as e:
        pass  # Disconnected so we can't send the message back.


@app.on_s3_event(bucket="RAW_DATA", events=["s3:ObjectCreated:*"])
def bulk_upload(event):
    # records to be fetched from S3
    bucket = s3.Bucket(event.bucket)
    response = bucket.Object(key=event.key).get()
    # reach and parse the response
    records = response["Body"].read().decode("utf-8").split()
    # add it to the database
    get_db().add_bulk_records(records)


@app.schedule(Rate(7, unit=Rate.DAYS))
def email_report():
    db_ref = get_db()
    records = db_ref.fetch_report_data()
    email.send_email(records)


# incase you want to send back a response
# app.websocket_api.send(
#     connection_id=event.connection_id,
#     message=event.body,
# )
