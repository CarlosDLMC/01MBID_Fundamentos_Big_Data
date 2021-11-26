from twython import Twython #Necesario instalarlo la primera vez de forma aislada: pip install Twython

APP_KEY = 'PyPLZfkAx7n9hTm7BfOD996fu' # API Key
APP_SECRET = '8hCuVQRAVAhQGEJ649BZCHDCoHxaJ8PB3kOwAcJxM6mSVvzdZc' # API Secret Key
OAUTH_TOKEN = '1254863830682873863-BNHo8U504mKS1YELiZeMGDZ1Krfw22' # Access Token
OAUTH_TOKEN_SECRET = 'UnUaFxL5wLDOkJVoJDHcqb1GNhCCzQJKL9FyVfoyCp3Q1' # Access Token Secret

twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)


search = twitter.search(q='#DOGE')

tweets = search['statuses']

print(len(tweets))