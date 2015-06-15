#!/usr/bin/env python
'''
Created on Jun 15, 2013

@author: mmartin
'''

from gi.repository import Gtk
from .handlers import Handlers
from CDMIConstants.constants import (
    HELP_DIALOG,
    HELP_FILE,
    HELP_TEXTVIEW,
    HELP_UI
)


class CDMIHelp(object):

    def __init__(self, session):
        '''
        Display the Help dialog
        '''
        self.session = session
        ppath = self.session.ppath
        handlers = Handlers()
        builder = Gtk.Builder()
        builder.add_from_file(HELP_UI % ppath)
        helpDialog = builder.get_object(HELP_DIALOG)
        builder.connect_signals(handlers)
        textview = builder.get_object(HELP_TEXTVIEW)
        textbuffer = textview.get_buffer()
        with open(HELP_FILE % ppath, 'r') as f:
            textbuffer.set_text(f.read())
        helpDialog.show_all()
        Gtk.main()
        helpDialog.hide()
