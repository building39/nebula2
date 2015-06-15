'''
Created on Jun 9, 2013

@author: mmartin
'''

import sys

from base64 import b64encode
from gi.repository import Gtk

from CDMIConstants.constants import (
    CONNECT_PASSWD,
    CONNECT_URL,
    CONNECT_USERID,
    ROOT
)


class Handlers(object):
    '''
    classdocs
    '''

    def __init__(self, session, builder):
        self.builder = builder
        self.session = session

    def onCancelConnect(self, *args):
        self.onQuit(*args)

    def onConnectOK(self, *args):
        entry = self.builder.get_object(CONNECT_PASSWD)
        self.session.cdmiPasswd = entry.get_text()
        entry = self.builder.get_object(CONNECT_URL)
        self.session.cdmiUrl = '%s/cdmi' % entry.get_text()
        entry = self.builder.get_object(CONNECT_USERID)
        self.session.cdmiUserid = entry.get_text()

        # prepend url with http if it's not already there
        if (self.session.cdmiUrl.startswith('http:') or
           self.session.cdmiUrl.startswith('https:')):
            pass
        else:
            self.session.cdmiUrl = 'http://%s' % self.session.cdmiUrl

        self.session.auth_basic = "Basic %s" % b64encode("%s:%s" %
                    (self.session.cdmiUserid, self.session.cdmiPasswd))

        self.session.headers['Authorization'] = self.session.auth_basic

        data = self.session.GET(path=ROOT)

        if data:
            self.session.cdmimodel = Gtk.TreeStore(str, str)
            self.session.displaymodel = Gtk.TreeStore(str)
            treeview = self.session.main_builder.get_object('cdmiview')
            treeview.set_enable_tree_lines(True)
            self.session.rootiter = self.session.cdmimodel.append(
                None,
                ['cdmi', '/']
            )
            self.session.displayroot = self.session.displaymodel.append(
                None,
                ['cdmi'])
            self.session.current_iter = self.session.displayroot
            self.session.render_treeview(treeview)
            treepath = self.session.cdmimodel.get_path(self.session.rootiter)
            self.session.get_children(treeview,
                                      treepath,
                                      data)
            self.session.display_cdmi_data(data)

        Gtk.main_quit()

    def onDeleteWindow(self, *args):
        self.onQuit(*args)

    def onQuit(self, *args):
        Gtk.main_quit()
