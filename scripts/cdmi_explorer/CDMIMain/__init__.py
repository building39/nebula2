#!/usr/bin/env python
'''
Created on Jun 8, 2013

@author: mmartin
'''
from gi.repository import Gtk
import sys

from handlers import Handlers
from CDMIConstants.constants import (
    DEBUGPATH
)

from CDMISession import CDMISession
from UIMenu import UI_MENU


class CDMIMainWindow(object):

    def __init__(self, ppath):
        # Build the main window
        sys.path.append(DEBUGPATH)
        self.ppath = ppath  # Where we live. Need this to find .glade files
        builder = Gtk.Builder()
        self.session = CDMISession(ppath, builder)
        self.handlers = Handlers(self.session)
        builder.add_from_file('%s/CDMIMain/CDMIMain.glade' % self.ppath)
        builder.connect_signals(self.handlers)
        self.window = builder.get_object('mainWindow')
        self.topBox = builder.get_object('topBox')
        self.menuBox = builder.get_object('menuBox')

        self.build_menu()
        # Go to work.

        self.window.show_all()
        Gtk.main()

    def add_CDMI_menu_actions(self, action_group):
        action_group.add_actions([
            ('CDMIMenu', None, 'CDMI'),
            ('CDMIConnect', Gtk.STOCK_COPY, 'Connect...', None, None,
             self.handlers.onConnect),
            ('CDMIQuit', None, 'Exit', None, None,
             self.handlers.onQuit)])

    def add_about_menu_actions(self, action_group):
        action_group.add_actions([
            ('AboutMenu', None, 'About'),
            ('About', Gtk.STOCK_COPY, 'About', None, None,
             self.handlers.onAbout),
            ('Help', None, 'Help', None, None,
             self.handlers.onHelp)])

    def build_menu(self):
        action_group = Gtk.ActionGroup("my_actions")

        self.add_CDMI_menu_actions(action_group)
        self.add_about_menu_actions(action_group)

        uimanager = self.create_ui_manager()
        uimanager.insert_action_group(action_group)

        menubar = uimanager.get_widget("/MenuBar")

        box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL)
        box.pack_start(menubar, False, False, 0)

        self.menuBox.add(box)

    def create_ui_manager(self):
        uimanager = Gtk.UIManager()

        # Throws exception if something went wrong
        uimanager.add_ui_from_string(UI_MENU)

        # Add the accelerator group to the toplevel window
        accelgroup = uimanager.get_accel_group()
        if accelgroup:
            self.window.add_accel_group(accelgroup)
        return uimanager
