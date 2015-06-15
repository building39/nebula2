#!/usr/bin/env python
'''
Created on Jun 8, 2013

@author: mmartin
'''

from gi.repository import Gtk
from CDMIConstants.constants import(
     ABOUT_DIALOG,
     ABOUT_UI
)


class CDMIAbout(object):

    def __init__(self, session):
        '''
        Display the About dialog
        '''
        self.session = session
        builder = Gtk.Builder()
        builder.add_from_file(ABOUT_UI % self.session.ppath)
        about = builder.get_object(ABOUT_DIALOG)
        about.run()
        about.hide()
