"""Tools package for DBQT."""

from .colcompare import main as compare_main
from .dbstats import main as dbstats_main

def compare(args):
    """Run column comparison tool"""
    compare_main(args)

def dbstats(args):
    """Run database statistics tool"""
    dbstats_main(args)
