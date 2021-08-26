# Copyright 2018 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import datetime

from firebase_admin import messaging

import requests
import json
import firebase_admin
from firebase_admin import credentials
from firebase_admin import messaging
from oauth2client.service_account import ServiceAccountCredentials
import os

def notify_topic_subscribers(topic, message):
      SCOPES = ['https://www.googleapis.com/auth/firebase.messaging']
      cred = ServiceAccountCredentials.from_json_keyfile_name(os.environ['config_path']+'poultryrate-311919-firebase-adminsdk-rbvsh-828d6dabc0.json', SCOPES)
      access_token_info = cred.get_access_token()

      headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token_info.access_token,
      }

      body = {
        "message": {
          "topic": topic,
          "notification": {
            "title": "Suggestion",
            "body": message
          },
          "data": {
            "story_id": "story_12345"
          },
          "android": {
            "notification": {
              "click_action": "TOP_STORY_ACTIVITY"
            }
          },
          "apns": {
            "payload": {
              "aps": {
                "category" : "NEW_MESSAGE_CATEGORY"
              }
            }
          }
        }
      }
      try:
        firebase_admin.get_app()
      except ValueError:
        cred = credentials.Certificate(os.environ['config_path']+'poultryrate-311919-firebase-adminsdk-rbvsh-828d6dabc0.json')
        firebase_admin.initialize_app(cred)

      response = requests.post("https://fcm.googleapis.com/v1/projects/poultryrate-311919/messages:send",headers = headers, data=json.dumps(body))
      print(response.status_code)

      print(response.json())
      return response

def send_to_token():
    # [START send_to_token]
    # This registration token comes from the client FCM SDKs.
    registration_token = 'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY'

    # See documentation on defining a message payload.
    message = messaging.Message(
        data={
            'score': '850',
            'time': '2:45',
        },
        token=registration_token,
    )

    # Send a message to the device corresponding to the provided
    # registration token.
    response = messaging.send(message)
    # Response is a message ID string.
    print('Successfully sent message:', response)
    # [END send_to_token]


def send_to_topic():
    # [START send_to_topic]
    # The topic name can be optionally prefixed with "/topics/".
    topic = 'highScores'

    # See documentation on defining a message payload.
    message = messaging.Message(
        data={
            'score': '850',
            'time': '2:45',
        },
        topic=topic,
    )

    # Send a message to the devices subscribed to the provided topic.
    response = messaging.send(message)
    # Response is a message ID string.
    print('Successfully sent message:', response)
    # [END send_to_topic]


def send_to_condition():
    # [START send_to_condition]
    # Define a condition which will send to devices which are subscribed
    # to either the Google stock or the tech industry topics.
    condition = "'Attock' in topics || 'Badin' in topics"

    # See documentation on defining a message payload.
    message = messaging.Message(
        notification=messaging.Notification(
            title='$GOOG up 1.43% on the day',
            body='$GOOG gained 11.80 points to close at 835.67, up 1.43% on the day.',
        ),
        condition=condition,
    )

    # Send a message to devices subscribed to the combination of topics
    # specified by the provided condition.
    response = messaging.send(message)
    # Response is a message ID string.
    print('Successfully sent message:', response)
    # [END send_to_condition]


def send_dry_run():
    message = messaging.Message(
        data={
            'score': '850',
            'time': '2:45',
        },
        token='token',
    )

    # [START send_dry_run]
    # Send a message in the dry run mode.
    response = messaging.send(message, dry_run=True)
    # Response is a message ID string.
    print('Dry run successful:', response)
    # [END send_dry_run]


def android_message():
    # [START android_message]
    message = messaging.Message(
        android=messaging.AndroidConfig(
            ttl=datetime.timedelta(seconds=3600),
            priority='normal',
            notification=messaging.AndroidNotification(
                title='$GOOG up 1.43% on the day',
                body='$GOOG gained 11.80 points to close at 835.67, up 1.43% on the day.',
                icon='stock_ticker_update',
                color='#f45342'
            ),
        ),
        topic='Attock',
    )
    # [END android_message]
    return message


def apns_message():
    # [START apns_message]
    message = messaging.Message(
        apns=messaging.APNSConfig(
            headers={'apns-priority': '10'},
            payload=messaging.APNSPayload(
                aps=messaging.Aps(
                    alert=messaging.ApsAlert(
                        title='$GOOG up 1.43% on the day',
                        body='$GOOG gained 11.80 points to close at 835.67, up 1.43% on the day.',
                    ),
                    badge=42,
                ),
            ),
        ),
        topic='industry-tech',
    )
    # [END apns_message]
    return message


def webpush_message():
    # [START webpush_message]
    message = messaging.Message(
        webpush=messaging.WebpushConfig(
            notification=messaging.WebpushNotification(
                title='$GOOG up 1.43% on the day',
                body='$GOOG gained 11.80 points to close at 835.67, up 1.43% on the day.',
                icon='https://my-server/icon.png',
            ),
        ),
        topic='industry-tech',
    )
    # [END webpush_message]
    return message


def all_platforms_message():
    # [START multi_platforms_message]
    message = messaging.Message(
        notification=messaging.Notification(
            title='$GOOG up 1.43% on the day',
            body='$GOOG gained 11.80 points to close at 835.67, up 1.43% on the day.',
        ),
        android=messaging.AndroidConfig(
            ttl=datetime.timedelta(seconds=3600),
            priority='normal',
            notification=messaging.AndroidNotification(
                icon='stock_ticker_update',
                color='#f45342'
            ),
        ),
        apns=messaging.APNSConfig(
            payload=messaging.APNSPayload(
                aps=messaging.Aps(badge=42),
            ),
        ),
        topic='industry-tech',
    )
    # [END multi_platforms_message]
    return message


def subscribe_to_topic():
    topic = 'highScores'
    # [START subscribe]
    # These registration tokens come from the client FCM SDKs.
    registration_tokens = [
        'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY_1',
        # ...
        'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY_n',
    ]

    # Subscribe the devices corresponding to the registration tokens to the
    # topic.
    response = messaging.subscribe_to_topic(registration_tokens, topic)
    # See the TopicManagementResponse reference documentation
    # for the contents of response.
    print(response.success_count, 'tokens were subscribed successfully')
    # [END subscribe]


def unsubscribe_from_topic():
    topic = 'highScores'
    # [START unsubscribe]
    # These registration tokens come from the client FCM SDKs.
    registration_tokens = [
        'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY_1',
        # ...
        'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY_n',
    ]

    # Unubscribe the devices corresponding to the registration tokens from the
    # topic.
    response = messaging.unsubscribe_from_topic(registration_tokens, topic)
    # See the TopicManagementResponse reference documentation
    # for the contents of response.
    print(response.success_count, 'tokens were unsubscribed successfully')
    # [END unsubscribe]


def send_all():
    registration_token = 'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY'
    # [START send_all]
    # Create a list containing up to 500 messages.
    messages = [
        messaging.Message(
            notification=messaging.Notification('Price drop', '5% off all electronics'),
            token=registration_token,
        ),
        # ...
        messaging.Message(
            notification=messaging.Notification('Price drop', '2% off all books'),
            topic='readers-club',
        ),
    ]

    response = messaging.send_all(messages)
    # See the BatchResponse reference documentation
    # for the contents of response.
    print('{0} messages were sent successfully'.format(response.success_count))
    # [END send_all]


def send_multicast():
    # [START send_multicast]
    # Create a list containing up to 500 registration tokens.
    # These registration tokens come from the client FCM SDKs.
    registration_tokens = [
        'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY_1',
        # ...
        'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY_N',
    ]

    message = messaging.MulticastMessage(
        data={'score': '850', 'time': '2:45'},
        tokens=registration_tokens,
    )
    response = messaging.send_multicast(message)
    # See the BatchResponse reference documentation
    # for the contents of response.
    print('{0} messages were sent successfully'.format(response.success_count))
    # [END send_multicast]


def send_multicast_and_handle_errors():
    # [START send_multicast_error]
    # These registration tokens come from the client FCM SDKs.
    registration_tokens = [
        'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY_1',
        # ...
        'cR0iRMn3S4KAfhn75SBbjX:APA91bGhU8PTvHO8rcFtrmCQ_zqS-TW7tuwevIvxnJjFLwLVkksL8zUQ01bUlqu0kYJ4O4LoUb1EmnK4W7HSwF4NoEq3qenLlBtIEajBb4gJ8JxF-v9Ir-dIqu8BBQKFcMuV7xavk_yY_N',
    ]

    message = messaging.MulticastMessage(
        data={'score': '850', 'time': '2:45'},
        tokens=registration_tokens,
    )
    response = messaging.send_multicast(message)
    if response.failure_count > 0:
        responses = response.responses
        failed_tokens = []
        for idx, resp in enumerate(responses):
            if not resp.success:
                # The order of responses corresponds to the order of the registration tokens.
                failed_tokens.append(registration_tokens[idx])
        print('List of tokens that caused failures: {0}'.format(failed_tokens))
    # [END send_multicast_error]
