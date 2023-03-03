#!/usr/bin/env python
#|-----------------------------------------------------------------------------
#|            This source code is provided under the Apache 2.0 license      --
#|  and is provided AS IS with no warranty or guarantee of fit for purpose.  --
#|                See the project's LICENSE.md for details.                  --
#|            Copyright (C) 2018-2021 Refinitiv. All rights reserved.        --
#|-----------------------------------------------------------------------------

"""
  This example demonstrates Warm-Standby functionality using WebSockets
  and a Refinitiv Real-Time service endpoint to retrieve market content.
  
  This is a minimal example, and does not implement service based failover.
"""

import requests
import json
import time
import sys
from Channel import Channel


# constants
PRIMARY = 0
STANDBY = 1


# User Variables
user     = '--- YOUR RTO MACHINE ID ---'
password = '--- Your RTO MACHINE PASSWORD ---'
clientid = '--- YOUR CLIENT ID OR APP ID ---'
servers = {
	'CH1': {
		'hostname': 'us-east-1-aws-1-lrg.optimized-pricing-api.refinitiv.net:443',
		'role': PRIMARY
	},
	'CH2': {
		'hostname': 'us-east-1-aws-2-lrg.optimized-pricing-api.refinitiv.net:443',
		'role': STANDBY
	}
}
subscriptions = [{
		'rics': ['IBM.N', 'GE.N'],
		'service': 'ELEKTRON_DD',
		'fields': ['BID', 'ASK', 'BIDSIZE', 'ASKSIZE', 'QUOTIM', 'TRDPRC_1', 'TRDVOL_1', 'SEQNUM']
	},{
		'rics': 'TD.TO',
		'service': 'ELEKTRON_DD',
		'fields': ['BID', 'ASK', 'TRDPRC_1']
	},{
		'rics': 'CAD=',
		'service': 'ELEKTRON_DD',
		'fields': None
	}]



class Controller:
	#========================================
	def authenticate(self, refreshToken):
	#========================================
		auth_url = 'https://api.refinitiv.com:443/auth/oauth2/v1/token'
		client_secret = ''
		scope = ''
	
		# get an OAuth access token
		if refreshToken is None:
			tData = {
				"username": user,
				"password": password,
				"grant_type": "password",
				"scope": scope,
				"takeExclusiveSignOnControl": "true"
			}
		else:
			tData = {
				"refresh_token": refreshToken,
				"grant_type": "refresh_token",
			}
			
		# Make a REST call to get latest access token
		response = requests.post(
			auth_url,
			headers = {
				"Accept": "application/json"
			},
			data = tData, 
			auth = (
				clientid,
				client_secret
			)
		)

		if (response.status_code == 400) and ('invalid_grant' in response.text):
			return None
		
		if response.status_code != 200:
			raise Exception("Failed to get access token {0} - {1}".format(response.status_code, response.text))

		self.oAuth = response.json()



	#========================================
	def channelUp(self, channel):
	#========================================
		print(channel.name + ': Channel Up')
		channel.login(self.oAuth['access_token'])



	#========================================
	def channelDown(self, channel):
	#========================================
		print(channel.name + ': Channel Down')
		# is it a primary or standby channel
		if channel.role == PRIMARY:
			# change the operation mode of the channels
			for chs in self.channels:
				if chs.role == PRIMARY:
					chs.setOpMode(STANDBY)
				else: 
					chs.setOpMode(PRIMARY)
		
		# try to restart this connection
		channel.connect()



	#========================================
	def __init__(self):
	#========================================
		self.oAuth = None
		self.channels = []



	#========================================
	def start(self):
	#========================================
		# get OAuth token
		print('Getting the oAuth token')
		self.authenticate(None)
		
		# instantiate the connection to the servers
		for channelName in servers:
			ch = Channel(channelName, servers[channelName]['hostname'], servers[channelName]['role'], self)
			self.channels.append(ch)
			ch.connect()
			for subscription in subscriptions:
				ch.subscribe(subscription)

		# token refresh loop
		while True:
			#  sleep for 90% of expiry time
			time.sleep(int(float(self.oAuth['expires_in']) * 0.90))
			# authn using refresh token
			self.authenticate(self.oAuth['refresh_token'])
			# channels -> relogin 
			for channel in self.channels:
				channel.login(self.oAuth['access_token'], True)



#========================================
if __name__ == "__main__":
#========================================
	Controller().start()
