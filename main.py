import threading
import json
import time
import google.auth.transport.requests
import google.auth.crypt
import google.auth.jwt
import google
from google.oauth2 import service_account
import requests
import pandas as pd
import concurrent.futures
import numpy as np
from constants import Constants


class sendPushNotification:
        
   def run(self):
        print("Script Started")
        time.sleep(10)
        excel_file = 'Data.xlsx'
       
        results = []    
        # df = pd.read_excel(excel_file, dtype=dtype)
        df = pd.read_excel(excel_file)
        num_processes = 4
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_processes) as executor:
           executor.map(self.process_row,df.iterrows())

        push_notification_result = pd.DataFrame(results)
        push_notification_result.to_excel("Result.xlsx", index=False)
        print(f"Data saved to Result.xlsx")
        print("Script Completed")

   def process_row(self,row):
        row = row[1]
        data = {}
        try:
            print("inside try")
            token = row["Token"]
            title = row["Title"]
            body = row["Body"]
            image = row["Image"]

            response = self.send_push_notification(token, title, body, image)
            response_data = json.loads(response.text)
            print(response)
            if response.status_code == 200:
                data['Response Code'] = response.status_code
                data['Message'] = response_data
            else:
                error = response_data["error"]
                data['Response Code'] = response.status_code
                data['Status'] = error["status"]
                data['Message'] = error["message"]
            global results
            results.append(data)
            push_notification_result = pd.DataFrame(results)
            print("into result ")
            push_notification_result.to_excel("Result.xlsx", index=False)
        except Exception as e:
            data['Message'] = "Error: " + str(e)
            results.append(data)
            push_notification_result = pd.DataFrame(results)
            push_notification_result.to_excel("Result.xlsx", index=False)

   def send_push_notification(self,token, title, body, image):
      api_url = 'https://fcm.googleapis.com/fcm/send'
      print("inside push notification")
      headers = {
         'Authorization': Constants.Auth_Token
      }
      payload = {
         
             "to": token,
             "notification": {
                 "title": title,
                 "body": body,
                 "image": image
             },
             "data": {
                 "payload": {
                     "screenName": "InvoiceUploadScreen"
                 }
             }
       }
      
      try:
          print(payload)
          response = requests.post(api_url, headers=headers, json=payload)
          return response
      except Exception as e:
          print("Exception while calling API for Token:")
          return e

if __name__ == '__main__':
    push_notification_instance = sendPushNotification()
    push_notification_instance.run()
