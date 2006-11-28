#! /usr/bin/env python
"""
logReceiver - class from Python Library reference section 6.29.4

Author:  Michelle Miller
Date:    Aug. 3, 2006

Central server receiving log messages from distributed machines and
writing to one central file where file order is based on receipt order.
"""
import cPickle
import logging
import logging.handlers
import SocketServer
import socket
import struct

class LogRecordStreamHandler (SocketServer.StreamRequestHandler):
   """
   Handler for a streaming logging request

   This basically logs the record using whatever logging policy is configured
   locally.
   """
   def handle (self):
      """
      Handle multiple requests - each expected to be a 4-byte length,
      followed by the LogRecord in pickle format.  Logs the record
      according to whatever policy is configured locally.
      """
      print "starting LogRecordStreamHandler.handle()"
      while 1:
         chunk = self.connection.recv(4)
         if len(chunk) < 4:
            break
         slen = struct.unpack (">L", chunk)[0]
         chunk = self.connection.recv (slen)
         while len(chunk) < slen:
            chunk = chunk + self.connection.recv (slen - len(chunk))
         obj = self.unPickle (chunk)
         record = logging.makeLogRecord (obj)
         self.handleLogRecord (record)

   def unPickle (self, data):
      return cPickle.loads (data)

   def handleLogRecord (self, record):
      
      if self.server.logname is not None:  
         name = self.server.logname
      else:
         name = record.name

      logger = logging.getLogger (name)
      logger.handle (record)

class LogRecordSocketReceiver (SocketServer.ThreadingTCPServer):
   """
   Simple TCP socket-based logging receiver suitable for testing.
   """
   allow_reuse_address = 1
   def __init__ (self, host = 'localhost',
                 port = logging.handlers.DEFAULT_TCP_LOGGING_PORT,
                 handler = LogRecordStreamHandler):
      print "Creating logRecordSocketReceiver on host=%r port=%s" % (host,port)
      SocketServer.ThreadingTCPServer.__init__ (self, (host, port), handler)
      self.abort = 0
      self.timeout = 1
      self.logname = None

   def serve_until_stopped (self):
      import select
 
      abort = 0
      while not abort:
         rd, wr, ex = select.select ([self.socket.fileno()], [], [],
                                     self.timeout) 
         if rd:
            self.handle_request()
         abort = self.abort

def main():
   # send all log messages to file in this format
   logging.basicConfig(level=logging.DEBUG,
          format = "%(asctime)s %(name)-15s %(levelname)-8s %(message)s",
          filename='/tmp/lsst.log',
          filemode='w')

   tcpserver = LogRecordSocketReceiver(
          host = socket.gethostname(),
          port = 2151,
   )
   print "About to start TCP server..."
   tcpserver.serve_until_stopped()

if __name__ == "__main__":
   main()

