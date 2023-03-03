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
"""

import websocket
import threading
import json
import sys

class Channel:
	
	#========================================
	def __init__(self, name, hostname, role, controller):
	#========================================
		self.name = name
		self.hostname = hostname
		self.role = role
		self.controller = controller
		# login is ID = 1
		self.id = 2
		self.subscriptions = []
		self.connected = False
		self.loggedIn = False



	#========================================
	def on_message(self, _, message):
	#========================================
		print(self.name + ': RECEIVED: ')
		message_json = json.loads(message)
		print(json.dumps(message_json, sort_keys=True, indent=2, separators=(',', ':')))

		for singleMsg in message_json:
			self.process_message(singleMsg)



	#========================================
	def on_error(self, _, error):
	#========================================
		print(self.name + ': Error')
		print(error)



	#========================================
	def on_close(self, _, close_status_code, close_msg):
	#========================================
		print(self.name + ': WebSocket closed')
		self.connected = False
		self.loggedIn = False
		self.controller.channelDown(self)



	#========================================
	def on_open(self, _):
	#========================================
		print(self.name + ': WebSocket connected!')
		self.connected = True
		self.controller.channelUp(self)



	#========================================
	def connect(self):
	#========================================
		# Start websocket handshake
		ws_address = 'wss://{}/WebSocket'.format(self.hostname)
		print(self.name + ': connecting to WebSocket ' + ws_address + ' ...')
		self.web_socket_app = websocket.WebSocketApp(ws_address, 
			on_message = self.on_message,
			on_error = self.on_error,
			on_close = self.on_close,
			on_open = self.on_open,
			subprotocols=['tr_json2'])

		# Event loop
		wst = threading.Thread(target = self.web_socket_app.run_forever, kwargs={'sslopt': {'check_hostname': False}})
		wst.start()
	

	
	#========================================
	def login(self, token, is_refresh = False):
	#========================================
		self.token = token
		
		login_json = {
			'ID': 1,
			'Domain': 'Login',
			'Key': {
				'NameType': 'AuthnToken',
				'Elements': {
					'ApplicationId': '255',
					'Position': '127.0.0.1/net',
					'AuthenticationToken': token
				}
			}
		}

		if is_refresh:
			login_json['Refresh'] = False
		
		self._sendJSON(login_json)



	#========================================
	def setOpMode(self, role):
	#========================================
		self.role = role
		
		if self.loggedIn:
			opMode_json = {
				'ID': 1,
				'Type': 'Generic',
				'Domain': 'Login',
				'Key': {
					'Name': 'ConsumerConnectionStatus'
				},
				'Complete': False,
				'Map': {
					'KeyType': 'AsciiString',
					'Entries': [{
							'Action': 'Add',
							'Key': 'WarmStandbyInfo',
							'Elements': {
								'WarmStandbyMode': self.role
							}
						}
					]
				}
			}
			
			self._sendJSON(opMode_json)



	#========================================
	def subscribe(self, item):
	#========================================
		self.subscriptions.append(item)
		self._subscribe(item)



	#========================================
	def _subscribe(self, item):
	#========================================
		if self.loggedIn:
			rics = item['rics']
		
			mp_req_json = {
				'ID': self.id,
				'Key': {
					'Name': rics,
					'Service': item['service']
				},
			}

			# limit the fields if the view is set
			if item['fields'] is not None:
				mp_req_json['View'] = item['fields']

			# increment the ID counter by the number of items requested
			if type(rics) is list:
				self.id += len(rics)

			self.id += 1
			self._sendJSON(mp_req_json)



	#========================================
	def process_message(self, message_json):
	#========================================
		message_type = message_json['Type']

		if message_type == 'Refresh':
			if 'Domain' in message_json:
				message_domain = message_json['Domain']
				if message_domain == 'Login':
					self.process_login_response(message_json)
		elif message_type == 'Ping':
			self._sendJSON({'Type': 'Pong'})



	#========================================
	def process_login_response(self, message_json):
	#========================================
		if message_json['State']['Stream'] != 'Open' or message_json['State']['Data'] != 'Ok':
			print(self.name + ': Login failed.')
			sys.exit(1)

		self.loggedIn = True
		
		# check if the server supports warm-standby
		if message_json['Key']['Elements']['SupportStandby'] == 1:
			# set the server operation mode
			self.setOpMode(self.role)
		else:
			print(self.name + ': !!!--- THIS SERVER DOES NOT SUPPORT WARM-STANDBY ---!!!')
		
		# subscribe to all the instruments
		for item in self.subscriptions:
			self._subscribe(item)



	#========================================
	def _sendJSON(self, jMsg):
	#========================================
		if self.connected:
			self.web_socket_app.send(json.dumps(jMsg))
			print(self.name + ': SENT:')
			print(json.dumps(jMsg, sort_keys=True, indent=2, separators=(',', ':')))

