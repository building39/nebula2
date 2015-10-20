# -*- coding: iso-8859-15 -*-
"""CDMI FunkLoad test

$Id$
"""
import unittest
from random import random
from funkload.FunkLoadTestCase import FunkLoadTestCase

class CDMI(FunkLoadTestCase):
    """This test use a configuration file CDMI.conf."""

    def setUp(self):
        """Setting up test."""
        self.server_url = self.conf_get('main', 'url')

    def test_cdmi(self):
        # The description should be set in the configuration file
        server_url = self.server_url
        # begin test ---------------------------------------------
        nb_time = self.conf_getInt('test_cdmi', 'nb_time')
        self.setBasicAuth('administrator', 'test')
        self.setHeader('x-cdmi-specification-version', '1.1')
        self.setHeader('Accept', 'application/cdmi-object')
        for i in range(nb_time):
            self.get(server_url, description='Get URL')
        # end test -----------------------------------------------
        self.clearBasicAuth()


if __name__ in ('main', '__main__'):
    unittest.main()
