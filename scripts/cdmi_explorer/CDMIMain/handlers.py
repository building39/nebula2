'''
Created on Jun 9, 2013

@author: mmartin
'''

import sys

from gi.repository import Gtk

from CDMIAbout import CDMIAbout
from CDMIConnect import CDMIConnect

from CDMIHelp import CDMIHelp


class Handlers(object):
    '''
    classdocs
    '''

    def __init__(self, session):
        self.session = session

    def onAbout(self, *args):
        CDMIAbout(self.session)

    def onConnect(self, *args):
        CDMIConnect(self.session)

    def onDeleteWindow(self, *args):
        self.onQuit(*args)

    def onHelp(self, *args):
        CDMIHelp(self.session)

    def onQuit(self, *args):
        Gtk.main_quit()

    def onCDMIRowCollapsed(self, *args):
        treeview = args[0]
        treeiter = args[1]
        treepath = args[2]
        model = treeview.get_model()
        print('URI1: %s', model[treeiter][1])
        data = self.session.GET(model[treeiter][1])
        self.session.get_children(treeview, treepath, data)
        self.session.display_cdmi_data(data)

    def onCDMIRowExpanded(self, *args):
        treeview = args[0]
        treeiter = args[1]
        treepath = args[2]
        rowname = self._squash_slashes(self.session.cdmimodel.get_value(treeiter, 1))
        print('URI2: %s', rowname)
        data = self.session.GET(rowname)
        treeiter = self.session.cdmimodel.get_iter(treepath)
        model = treeview.get_model()
        prefix = rowname
        #if prefix.endswith('/'):
        #    prefix = prefix[:-1]
        if model.iter_has_child(treeiter):
            num_children = model.iter_n_children(treeiter)
            for i in range(num_children):
                if not data:
                    break
                child = data['children'][i]
        #        if child.endswith('/'):
        #            child = child[:-1]
                childpath = self._squash_slashes('%s/%s' % (prefix, child))
                print('URI3: %s', childpath)
                childdata = self.session.GET(childpath)
                childiter = model.iter_nth_child(treeiter, i)
                self.session.get_children(treeview,
                                          model.get_path(childiter),
                                          childdata)
        self.session.display_cdmi_data(data)
        return

    def onCDMIRowActivated(self, *args):
        '''
        Display the CDMI data for the selected row.
        '''
        treeview = args[0]
        treepath = args[1]
        _column = args[2]
        model = treeview.get_model()
        treeiter = model.get_iter(treepath)
        print('URI4: %s', model[treeiter][1])
        data = self.session.GET(model[treeiter][1])
        self.session.get_children(treeview, treepath, data)
        self.session.display_cdmi_data(data)

    def onSelectCursorRow(self, *args):
        print 'onSelectCursorRow args: %s' % args
        sys.stdout.flush()

    def onCursorChanged(self, *args):
        print 'onCursorChanged args: %s' % args
        sys.stdout.flush()

    def _squash_slashes(self, S):
        T = ""
        for i in range(len(S)):
            try:
                if S[i] == '/' and S[i+1] == '/':
                    i += 1
                    continue
                T = T + S[i]
            except:
                T = T + S[i]
        return T
