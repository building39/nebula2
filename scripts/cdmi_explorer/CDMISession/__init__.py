'''
Created on Jun 14, 2013

@author: mmartin
'''

from gi.repository import Gtk
import json
import requests
import sys


class CDMISession(object):
    '''
    Session class
    '''

    def __init__(self, ppath, builder):
        '''
        Constructor
        '''
        self.cdmiUrl = None
        self.cdmiUserid = None
        self.cdmiPasswd = None
        self.current_iter = None  # treeiter for current node
        self.headers = {'X-CDMI-Specification-Version': '1.1'}
        self.main_builder = builder
        self.verify_cert = False
        self.ppath = ppath
        self.rootiter = None  # The root's treeiter

    def GET(self, path):
        '''
        Return CDMI values
        '''
        if not path.startswith('/'):
            path = '/%s' % path
        #if path.endswith('/'):
        #    path = path[:-1]  # Must strip trailing slash
        print('cdmiUrl: %s' % self.cdmiUrl)
        url = '%s%s' % (self.cdmiUrl, self._squash_slashes(path))

        statusbar = self.main_builder.get_object('statusbar1')
        res = requests.get(url=url,
                           allow_redirects=True,
                           headers=self.headers,
                           verify=self.verify_cert)

        if res.status_code in [200]:
            statusbar.push(0, 'Connected to %s' % self.cdmiUrl)
            return json.loads(res.text)
        else:
            statusbar.push(0, 'WTF?!? status=%s url=%s' % (res.status_code, url))
            return None

    def display_cdmi_data(self, data):
        cdmi_data_view = self.main_builder.get_object('cdmidata')
        text_buffer = cdmi_data_view.get_buffer()
        text_buffer.set_text(json.dumps(data,
                                        sort_keys=True,
                                        indent=4,
                                        separators=(',', ': ')))

    def get_children(self, treeview, treepath, data):
        model = treeview.get_model()
        treeiter = model.get_iter(treepath)
        if model.iter_has_child(treeiter):
            return  # we already got the children
        prefix = '%s' % data.get('parentURI', '/')
        print('get_children: parentURI is %s' % prefix)
        #if prefix.endswith('/'):
        #    prefix = prefix[:-1]
        object_name = data.get('objectName', '')
        print('get_children: objectName is %s' % object_name)
        #if object_name.endswith('/'):
        #    object_name = object_name[:-1]
        if len(object_name) > 0:
            prefix = '%s/%s' % (prefix, object_name)
        print('get_children: full name is %s' % prefix)
        num_children = len(data.get('children', []))
        for i in range(num_children):
            child = data['children'][i]
            #if child.endswith('/'):
            #    child = child[:-1]
            cdmipath = '%s/%s' % (prefix, child)
            _childiter = self.cdmimodel.append(treeiter,
                                       [child, cdmipath])
            self.displaymodel.append(treeiter, [child])

    def render_treeview(self, treeview):
        treeview.set_model(self.displaymodel)
        renderer_text = Gtk.CellRendererText()
        column_text = Gtk.TreeViewColumn("CDMI Tree", renderer_text, text=0)
        treeview.append_column(column_text)
        treeview.set_model(self.cdmimodel)


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
