#!usr/bin/env python

# import configuration
import asyncio 
import datetime
import time 
import logging
import math
import ssl
import sys
import json

API_KEY = 'AIzaSyChbhQwnuzHyjzfxTq18iZ-bIyQCcPBB-U'

GOOGLE_API_QUERY = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?"

GOOGLE_HOST = "maps.googleapis.com"

GOOGLE_QUERY = "/maps/api/place/nearbysearch/json?"

SERVER_IP = '127.0.0.1'

SERVER_PORT_NUMBERS = {
	 'Goloman': 15680, 
	 'Hands'  : 15681, 
	 'Holiday': 15682, 
	 'Welsh'  : 15683, 
	 'Wilkes' : 15684
}

SERVER_COMMUNICATIONS = {
	'Goloman' : ['Hands', 'Holiday', 'Wilkes'],
	'Hands'   : ['Wilkes', 'Goloman'],
	'Holiday' : ['Welsh', 'Wilkes', 'Goloman'],
	'Welsh'   : ['Holiday'],
	'Wilkes'  : ['Goloman', 'Hands', 'Holiday']
}

def set_up_logger(server_log):
	logger = logging.getLogger(server_log)
	logger.setLevel(logging.INFO)
	formatter = logging.Formatter('%(asctime)s  -  %(name)s - %(levelname)s - %(message)s')
	fh = logging.FileHandler(server_log + '.log', mode='w')
	fh.setFormatter(formatter)
	ch = logging.StreamHandler()
	ch.setFormatter(formatter)
	logger.addHandler(fh)
	logger.addHandler(ch)
	return logger 

def ret_context():
	ctxt = ssl.create_default_context()
	ctxt.check_hostname = False
	ctxt.verify_mode = ssl.CERT_NONE
	return ctxt

class ProtocolServerToServer(asyncio.Protocol):
	def __init__(self, message, name):
		self.message = message
		self.name = name

	def connection_made(self, transport):
		self.transport = transport
		self.transport.write(self.message.encode())
		logger.info('Connection to server {} made'.format(self.name))
		logger.info('Sent data to server {}\n'.format(self.name))

	def connection_lost(self, exc):
		self.transport.close()
		logger.info('The server {} connection closed \n'.format(self.name))

class ProtocolServerToClient(asyncio.Protocol):

	last_client_location = {}
	last_client_time = {}
	client_at = {}
	
	def __init__(self, name):
		self.name = name
		self.neighbours = SERVER_COMMUNICATIONS[name]
	
	def connection_made(self, transport):
		peername = transport.get_extra_info('peername')
		print('Connection from {}'.format(peername))
		self.transport = transport

	def data_received(self, data):
		messages = data.decode()
		logger.info('Data received: {}'.format(messages))
		for message in messages.split('\\n'):
			print(message)
			if message.split()[0] == 'IAMAT' and self.is_valid_IMAT(message.split()[1:]):
				res = self.IAMAT_response(message.split()[1], [message.split()[2], message.split()[3]]) + '\n'
			elif message.split()[0] == 'WHATSAT' and self.is_valid_WHATSAT(message.split()[1:]):
				self.Whatsat_response(message.split()[1], message.split()[2], message.split()[3], message)
				return
			elif message.split()[0] == 'AT':
				res = self.AT_Response(message, message.split()[3])
			else:
				res = '? ' + message + '\n'

			self.transport.write(res.encode())
			logger.info('Send: {}'.format(res))
		
		# self.transport.close()
		# peername = self.transport.get_extra_info('peername')
		# logger.info('Close connection from {}\n'.format(peername))

	def send_to_neighbours(self, message, avoid_neighbours):
		servers = list(set(self.neighbours) - set(avoid_neighbours))
		for server in servers:
			cr = loop.create_connection(lambda: ProtocolServerToServer(message, server), SERVER_IP, SERVER_PORT_NUMBERS[server])
			loop.create_task(cr)

	def find_pos(self, location):
		pos1 = -1 
		for c in range(len(location)):
			if location[c] == '+' or location[c] =='-':
				if pos1 != -1:
					return pos1, c
				else:
					pos1 = c

	def get_client_location(self, client):
		l_s = self.last_client_location[client]
		index1, index2 = self.find_pos(l_s)
		lat_s = l_s[index1: index2]
		long_s = l_s[index2:]
		return lat_s, long_s

	def is_ISO_Location(self, location):
		index1, index2 = self.find_pos(location)
		lat_s = location[index1: index2]
		long_s = location[index2:]
		if math.fabs(float(lat_s)) > 90 or math.fabs(float(long_s)) > 180:
			return False
		return True

	def get_client_time(self, client):
		time = self.last_client_time[client]
		return float(time)

	def is_Posix_Time(self, time):
		try:
			datetime.datetime.utcfromtimestamp(float(time))
			return True

		except ValueError:
			return False 

	def change_loc_and_time(self, client, at_message):
		if at_message[3] != client:
			logger.error('Incorrect use of change_loc_and_time')
			return False

		if ProtocolServerToClient.last_client_time.get(client, None):	
			if float(at_message[5]) > self.get_client_time(client):
				ProtocolServerToClient.last_client_time[client] = at_message[5]
				ProtocolServerToClient.last_client_location[client] = at_message[4]
				ProtocolServerToClient.client_at[client] = ' '.join(at_message)
				logger.info('Changed last location for {}'.format(client))
				return True
		else:
			ProtocolServerToClient.last_client_time[client] = at_message[5]
			ProtocolServerToClient.last_client_location[client] = at_message[4]
			ProtocolServerToClient.client_at[client] = ' '.join(at_message)
			logger.info('Changed last location for {}'.format(client))
			return True

		logger.warning('Unable to change last location for {}'.format(client))
		return False

	def is_valid_IMAT(self, params):
		if len(params) == 3:
			print (params[1])
			if self.is_ISO_Location(params[1]):
				if self.is_Posix_Time(params[2]):
					return True
				else:
					logger.error('Invalid time format passed to IMAT')
					return False
			else:
				logger.error('Invalid location format passed to IMAT')
				return False
		else:
			logger.error('IAMAT input error')
			return False
	
	def is_valid_WHATSAT(self, params):
		if len(params) == 3:
			if ProtocolServerToClient.last_client_location.get(params[0], None):
				try:
					if float(params[1]) > 50 or float(params[1]) < 0:
						logger.error('Invalid radius provided to WHATSAT')
						return False
					if int(params[2]) > 20 or int(params[2]) < 0:
						logger.error('Invalid bound provided to WHATSAT')
						return False
					return True

				except ValueError:
					logger.error('Invalid param type for WHATSAT')
					return False
			else:
				logger.error('No known location for client {} location'.format(params[0]))
				return False
		else:
			logger.error('WHATSAT input error')
			return False

	def AT_Response(self, message, client):
		peername = self.transport.get_extra_info('peername')
		svr, at_message = message.split()[len(message.split()) - 1], message.split()[0:6]
		logger.info('server {} is propagating data through {} connection'.format(svr, peername))
		self.change_loc_and_time(client, at_message)
		new_mess = message + ' ' + str(self.name)
		avoid_neighbours = new_mess.split()[5:]
		self.send_to_neighbours(new_mess, avoid_neighbours)
		return 'updated location received by {}'.format(self.name)
	
	def IAMAT_response(self, client, params):
		# params[0] - location, params[1] - time
		curr_time = time.time()
		diff = '{:.9f}'.format(curr_time - float(params[1]))
		if not (curr_time - float(params[1]) < 0):
			diff = '+' + diff
		at_message = ['AT', self.name, diff, client, params[0], params[1]]
		if self.change_loc_and_time(client, at_message):
			new_mess = ' '.join(at_message) + ' ' + self.name
			avoid_neighbours = new_mess.split()[5:]
			self.send_to_neighbours(new_mess, avoid_neighbours)
		else:
			logger.info('Location data failed to propogate')

		return ProtocolServerToClient.client_at[client]

	def Whatsat_response(self, client, radius, bound, err):
		lat_cl, long_cl = self.get_client_location(client)
		google_location = '{},{}'.format(lat_cl, long_cl)
		query = '{}location={}&radius={}&key={}'.format(GOOGLE_QUERY, google_location, str(float(radius) * 10 * 100), API_KEY)
		request = 'GET {} HTTP/1.1\r\n'.format(query) + 'Host: {}\r\n'.format(GOOGLE_HOST) + '\r\n'
		context = ret_context()
		protocol = lambda: ProtocolHTTP(self.transport, request,  ProtocolServerToClient.client_at[client],  int(bound))
		logger.info('HTTP request sent:' + request)
		cr = loop.create_connection(protocol, GOOGLE_HOST, 443 , ssl=context)
		loop.create_task(cr)

class ProtocolHTTP(asyncio.Protocol):
	
	def __init__(self, transport, req, header , bound):
		self.request = req
		self.bound = bound
		self.header = header
		self.init_transport = transport
		self.google_resp = ''

	def connection_made(self, transport):
		self.transport = transport
		self.transport.write(self.request.encode())

	def data_received(self, data):
		self.google_resp = self.google_resp + data.decode()
		if (self.google_resp.count('\r\n\r\n') >= 2):
			first, last = self.google_resp.split('\r\n\r\n')[1].index('{'), self.google_resp.split('\r\n\r\n')[1].rindex('}')
			result = self.google_resp.split('\r\n\r\n')[1][first: last+1].strip().replace('\r\n', '')
			result = result.replace('\n', '')
			j_o = json.loads(result)
			if len(j_o['results']) > self.bound:
				j_o['results'] = j_o['results'][:self.bound]
			j_s = self.header + '\n' + json.dumps(j_o, indent=2) + '\n\n' 
			self.init_transport.write(j_s.encode())
			logger.info('Send data: {}'.format(j_s))		
			# self.init_transport.close()
			peername = self.init_transport.get_extra_info('peername')
			# logger.info('Close connection from {}\n'.format(peername))
			self.transport.close()


def run_exception_handler(loop, context):
	try:
		e = context['exception']
		logger.error('Exception found: ' + str(context['exception']))
	except KeyError:
		logger.error('Exception found: ' + str(context['message']))

if __name__ == '__main__':

	name, port = sys.argv[1], SERVER_PORT_NUMBERS[sys.argv[1]]
	logger = set_up_logger(name)
	loop = asyncio.get_event_loop()
	loop.set_exception_handler(run_exception_handler)
	cr = loop.create_server(lambda: ProtocolServerToClient(name), SERVER_IP, port)
	server = loop.run_until_complete(cr)
	logger.info('Server running on {}'.format(server.sockets[0].getsockname()))
	try:
		loop.run_forever()
	except KeyboardInterrupt:
		pass

	server.close()
	loop.run_until_complete(server.wait_closed())
	loop.close()







