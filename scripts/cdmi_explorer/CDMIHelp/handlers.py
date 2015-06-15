'''
Created on Jun 9, 2013

@author: mmartin
'''
from gi.repository import Gtk


class Handlers(object):
    '''
    classdocs
    '''

    def onHelpOK(self, *args):
        Gtk.main_quit()

    def onDeleteWindow(self, *args):
        self.onQuit(*args)

    def onQuit(self, *args):
        Gtk.main_quit()
