
import json
import requests
import pandas as pd
import boto3
import datetime
from datetime import date
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from botocore.exceptions import ClientError

ses = boto3.client('ses')

SENDER = "Real Estate Listing <source@abc.com>"

# The subject line for the email.
SUBJECT = "Daily Listings"

# The email body for recipients with non-HTML email clients.
BODY_TEXT = ("Real Estate listings\r\n"
             "This email was sent with Amazon SES"
            )

# The HTML body of the email.
BODY_HTML = """<html>
<head></head>
<body>
  <h1>Real Estate listings</h1>
  <p>This email was sent with
    <a href='https://aws.amazon.com/ses/'>Amazon SES</a>
  </p>
</body>
</html>"""

# The character encoding for the email.
CHARSET = "UTF-8"


def getURL(searchParameter,propertyState):
    if searchParameter == 'Australia':
        return '{%22channel%22:%22' + propertyState + '%22}'

    searchQuery = '{%22channel%22:%22' + propertyState + '%22,%22filter%22:{%22replaceProjectWithFirstChild%22:true},%22localities%22:[{%22subdivision%22:%22'+ searchParameter +'%22}]}'
    return searchQuery

def lambda_handler(event, lambda_context):

    today = date.today()
    DAY = str(today.day)
    url = 'https://services.realestate.com.au/services/listings/search?query='
    searchParameters = ['Australia',
                        'NSW',
                        'QLD',
                        'VIC',
                        'SA',
                        'WA',
                        'TAS',
                        'ACT',
                        'NT',
                        'Wollongong']

    propertyStates = ['buy','rent']

    results = []
    for parameter in searchParameters:
        listings = []
        listingForSale = 0
        listingForRent = 0

        for propertyState in propertyStates:

            api_url = url + getURL(parameter,propertyState)
            response = requests.get(api_url)
            data = response.json()

            if propertyState == 'buy':
                listingForSale = data["totalResultsCount"]
            elif propertyState == 'rent':
                listingForRent = data["totalResultsCount"]
        listings.append(parameter)
        listings.append(listingForSale)
        listings.append(listingForRent)
        listings.append(listingForSale + listingForRent)
        results.append(listings)

    df = pd.DataFrame(results, columns=['Region', 'Buy', 'Rent', 'Total'])

    attachment_string = df.to_csv(index=False)
    message = MIMEMultipart()
    message['Subject'] = SUBJECT
    message['From'] = SENDER
    # message body
    part = MIMEText(BODY_HTML, 'html')
    message.attach(part)
    # attachment

    part = MIMEApplication(str.encode(attachment_string))
    part.add_header('Content-Disposition', 'attachment', filename='listings.csv')
    message.attach(part)

    #Sending as email
    response = ses.send_raw_email(
        Source=message['From'],
        Destinations=['destination@abc.com'],
        RawMessage={
            'Data': message.as_string()
        }
    )
    
    print(response)
    return json.dumps(df.to_csv())
