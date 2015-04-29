#!/usr/bin/python

import InverterMsg      # Import the Msg handler

import socket               # Needed for talking to inverter
import datetime             # Used for timestamp
import sys
import logging
import ConfigParser, os
import pickle
import struct
import time

# For PVoutput 
import urllib, urllib2

# Load the setting
mydir = os.path.dirname(os.path.abspath(__file__))

config = ConfigParser.RawConfigParser()
config.read([mydir + '/config-default.cfg', mydir + '/config.cfg'])

# Receive data with a socket
ip              = config.get('inverter','ip')
port            = config.get('inverter','port')
use_temp        = config.getboolean('inverter','use_temperature')
wifi_serial     = config.getint('inverter', 'wifi_sn')

mysql_enabled   = config.getboolean('mysql', 'mysql_enabled')
mysql_host      = config.get('mysql','mysql_host')
mysql_user      = config.get('mysql','mysql_user')
mysql_pass      = config.get('mysql','mysql_pass')
mysql_db        = config.get('mysql','mysql_db')

pvout_enabled   = config.getboolean('pvout','pvout_enabled')
pvout_apikey    = config.get('pvout','pvout_apikey')
pvout_sysid     = config.get('pvout','pvout_sysid')

log_enabled     = config.getboolean('log','log_enabled')
log_filename    = mydir + '/' + config.get('log','log_filename')

graphite_enabled = config.getboolean('graphite','graphite_enabled')
graphite_host   = config.get('graphite','graphite_host')
graphite_port   = config.getint('graphite', 'graphite_port')
graphite_delay  = config.getint('graphite','graphite_delay')


server_address = ((ip, port))

logger = logging.getLogger('OmnikLogger')
hdlr = logging.FileHandler(log_filename)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

for res in socket.getaddrinfo(ip, port, socket.AF_INET , socket.SOCK_STREAM):
    af, socktype, proto, canonname, sa = res
    try:
        if log_enabled:
            logger.info('connecting to %s port %s' % server_address)
        s = socket.socket(af, socktype, proto)
        s.settimeout(10)
    except socket.error as msg:
        s = None
        continue
    try:
        s.connect(sa)
    except socket.error as msg:
        s.close()
        s = None
        continue
    break
    
if s is None:
    if log_enabled:
        logger.error('could not open socket')
    sys.exit(1)
    
s.sendall(InverterMsg.generate_string(wifi_serial))
data = s.recv(1024)
s.close()

msg = InverterMsg.InverterMsg(data)  # This is where the magic happens ;)
now = datetime.datetime.now()

if log_enabled:
    logger.info("ID: {0}".format(msg.getID())) 


if mysql_enabled:
    # For database output
    import MySQLdb as mdb   
    
    if log_enabled:
        logger.info('Uploading to database')
    con = mdb.connect(mysql_host, mysql_user, mysql_pass, mysql_db);
    
    with con:
        cur = con.cursor()
        cur.execute("""INSERT INTO minutes 
        (InvID, timestamp, ETotal, EToday, Temp, HTotal, VPV1, VPV2, VPV3,
         IPV1, IPV2, IPV3, VAC1, VAC2, VAC3, IAC1, IAC2, IAC3, FAC1, FAC2, 
         FAC3, PAC1, PAC2, PAC3) 
        VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
         %s, %s, %s, %s, %s, %s, %s);""", 
         (msg.getID(), now, msg.getETotal(), 
          msg.getEToday(), msg.getTemp(), msg.getHTotal(), msg.getVPV(1), 
          msg.getVPV(2), msg.getVPV(3), msg.getIPV(1), msg.getIPV(2), 
          msg.getIPV(3), msg.getVAC(1), msg.getVAC(2), msg.getVAC(3), 
          msg.getIAC(1), msg.getIAC(2), msg.getIAC(3), msg.getFAC(1), 
          msg.getFAC(2), msg.getFAC(3), msg.getPAC(1), msg.getPAC(2), 
          msg.getPAC(3)) );

if pvout_enabled and (now.minute % 5) == 0:
    if log_enabled:
        logger.info('Uploading to PVoutput')
    url = "http://pvoutput.org/service/r2/addstatus.jsp"
    
    if use_temp:
        get_data = {
            'key': pvout_apikey, 
            'sid': pvout_sysid, 
            'd': now.strftime('%Y%m%d'),
            't': now.strftime('%H:%M'),
            'v1': msg.getEToday() * 1000,
            'v2': msg.getPAC(1),
            'v5': msg.getTemp(),
            'v6': msg.getVPV(1)
        }
    else:
        get_data = {
            'key': pvout_apikey, 
            'sid': pvout_sysid, 
            'd': now.strftime('%Y%m%d'),
            't': now.strftime('%H:%M'),
            'v1': msg.getEToday() * 1000,
            'v2': msg.getPAC(1),
            'v6': msg.getVPV(1)
        }

    get_data_encoded = urllib.urlencode(get_data)                       # UrlEncode the parameters
    
    request_object = urllib2.Request(url + '?' + get_data_encoded)      # Create request object
    response = urllib2.urlopen(request_object)                          # Make the request and store the response
    
    if log_enabled:
        logger.info(response.read())                                               # Show the response

if graphite_enabled and (now.minute % graphite_delay) == 0:
   if log_enabled:
        logger.info('Uploading to graphite')
   sock = socket.socket()
   try:
        sock.connect( (graphite_host, graphite_port) )    
   except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d" % {'server':graphite_host, 'port':graphite_port})

   now = int(time.time())
   tuples = ([])
   lines = []
#        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#         %s, %s, %s, %s, %s, %s, %s);""",
#         (msg.getID(), now, msg.getETotal(),
#          msg.getEToday(), msg.getTemp(), msg.getHTotal(), msg.getVPV(1),
#          msg.getVPV(2), msg.getVPV(3), msg.getIPV(1), msg.getIPV(2),
#          msg.getIPV(3), msg.getVAC(1), msg.getVAC(2), msg.getVAC(3),
#          msg.getIAC(1), msg.getIAC(2), msg.getIAC(3), msg.getFAC(1),
#          msg.getFAC(2), msg.getFAC(3), msg.getPAC(1), msg.getPAC(2),
#          msg.getPAC(3)) );
   
   tuples.append(('omnik.%s.ETotal'% msg.getID() , (now,msg.getETotal())))
   lines.append("omnik.%s.ETotal %s %d" % (msg.getID(), msg.getETotal(), now ))
   tuples.append(('omnik.%s.EToday'% msg.getID() , (now,msg.getEToday())))
   lines.append("omnik.%s.EToday %s %d" % (msg.getID(), msg.getEToday(), now ))
   tuples.append(('omnik.%s.Temp'% msg.getID() , (now,msg.getTemp())))
   lines.append("omnik.%s.Temp %s %d" % (msg.getID(), msg.getTemp(), now ))
   tuples.append(('omnik.%s.HTotal'% msg.getID() , (now,msg.getHTotal())))
   lines.append("omnik.%s.HTotal %s %d" % (msg.getID(), msg.getHTotal(), now ))
   tuples.append(('omnik.%s.VPV1'% msg.getID() , (now,msg.getVPV(1))))
   lines.append("omnik.%s.VPV1 %s %d" % (msg.getID(), msg.getVPV(1), now ))
   tuples.append(('omnik.%s.VPV2'% msg.getID() , (now,msg.getVPV(2))))
   lines.append("omnik.%s.VPV2 %s %d" % (msg.getID(), msg.getVPV(2), now ))
   tuples.append(('omnik.%s.VPV3'% msg.getID() , (now,msg.getVPV(3))))
   lines.append("omnik.%s.VPV3 %s %d" % (msg.getID(), msg.getVPV(3), now ))
   tuples.append(('omnik.%s.IPV1'% msg.getID() , (now,msg.getIPV(1))))
   lines.append("omnik.%s.IPV1 %s %d" % (msg.getID(), msg.getIPV(1), now ))
   tuples.append(('omnik.%s.IPV2'% msg.getID() , (now,msg.getIPV(2))))
   lines.append("omnik.%s.IPV2 %s %d" % (msg.getID(), msg.getIPV(2), now ))
   tuples.append(('omnik.%s.IPV3'% msg.getID() , (now,msg.getIPV(3))))
   lines.append("omnik.%s.IPV3 %s %d" % (msg.getID(), msg.getIPV(3), now ))
   tuples.append(('omnik.%s.VAC1'% msg.getID() , (now,msg.getVAC(1))))
   lines.append("omnik.%s.VAC1 %s %d" % (msg.getID(), msg.getVAC(1), now ))
   tuples.append(('omnik.%s.VAC2'% msg.getID() , (now,msg.getVAC(2))))
   lines.append("omnik.%s.VAC2 %s %d" % (msg.getID(), msg.getVAC(2), now ))
   tuples.append(('omnik.%s.VAC3'% msg.getID() , (now,msg.getVAC(3))))
   lines.append("omnik.%s.VAC3 %s %d" % (msg.getID(), msg.getVAC(3), now ))
   tuples.append(('omnik.%s.IAC1'% msg.getID() , (now,msg.getIAC(1))))
   lines.append("omnik.%s.IAC1 %s %d" % (msg.getID(), msg.getIAC(1), now ))
   tuples.append(('omnik.%s.IAC2'% msg.getID() , (now,msg.getIAC(2))))
   lines.append("omnik.%s.IAC2 %s %d" % (msg.getID(), msg.getIAC(2), now ))
   tuples.append(('omnik.%s.IAC3'% msg.getID() , (now,msg.getIAC(3))))
   lines.append("omnik.%s.IAC3 %s %d" % (msg.getID(), msg.getIAC(3), now ))
   tuples.append(('omnik.%s.FAC1'% msg.getID() , (now,msg.getFAC(1))))
   lines.append("omnik.%s.FAC1 %s %d" % (msg.getID(), msg.getFAC(1), now ))
   tuples.append(('omnik.%s.FAC2'% msg.getID() , (now,msg.getFAC(2))))
   lines.append("omnik.%s.FAC2 %s %d" % (msg.getID(), msg.getFAC(2), now ))
   tuples.append(('omnik.%s.FAC3'% msg.getID() , (now,msg.getFAC(3))))
   lines.append("omnik.%s.FAC3 %s %d" % (msg.getID(), msg.getFAC(3), now ))
   tuples.append(('omnik.%s.PAC1'% msg.getID() , (now,msg.getPAC(1))))
   lines.append("omnik.%s.PAC1 %s %d" % (msg.getID(), msg.getPAC(1), now ))
   tuples.append(('omnik.%s.PAC2'% msg.getID() , (now,msg.getPAC(2))))
   lines.append("omnik.%s.PAC2 %s %d" % (msg.getID(), msg.getPAC(2), now ))
   tuples.append(('omnik.%s.PAC3'% msg.getID() , (now,msg.getPAC(3))))
   lines.append("omnik.%s.PAC3 %s %d" % (msg.getID(), msg.getPAC(3), now ))
   message = '\n'.join(lines) + '\n' #all lines must end in a newline
   if log_enabled:
      logger.info('sending  message')
      logger.info('%s' % message)
   try:
      package = pickle.dumps(tuples, 1)
      size = struct.pack('!L', len(package))
      sock.sendall(size)
      sock.sendall(package)
   except socket.error:
        raise SystemExit("Couldn't send data to %(server)s on port %(port)d" % {'server':graphite_host, 'port':graphite_port})

