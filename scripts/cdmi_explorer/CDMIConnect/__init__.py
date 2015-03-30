'''
Created on Jun 9, 2013

@author: mmartin
'''

from gi.repository import Gtk
from .handlers import Handlers
from CDMIConstants.constants import (
    CONNECT_DIALOG,
    CONNECT_UI
)


class CDMIConnect(object):
    '''
    CDMI connection class
    '''

    def __init__(self, session):
        '''
        Constructor
        '''
        self.session = session
        builder = Gtk.Builder()
        self.handlers = Handlers(session, builder)
        builder.add_from_file(CONNECT_UI % self.session.ppath)
        connect = builder.get_object(CONNECT_DIALOG)
        builder.connect_signals(self.handlers)
        connect.show_all()
        Gtk.main()
        connect.hide()
