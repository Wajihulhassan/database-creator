#!/usr/bin/env python

import json
import gzip
import os
import sys
import binascii
import argparse
import traceback

EXCLUDE_LIST=["conn-summary.", "stdout.", "stderr.", "reporter.", "packet_filter.", "loaded_scripts."]

class fileHeader(object):
    def __init__(self):
        self.separator = '\t'
        self.unset = '-'
        self.empty = '(empty)'
        self.set_separator = ','
        self.path = None
        self.open = None
        self.fields = None
        self.types = None
        self.close = None


def convertBroType(inData, type, fh):
    if type.startswith('set') or type.startswith('vector'):
        innerType = type.split('[')[1][:-1]

        if inData == fh.empty:
            return []

        values = inData.split(fh.set_separator)
        outData = []
        for v in values:
            convertedData = convertBroSimple(v, innerType, fh)
            if convertedData is not None:
                outData.append(convertedData)
        return outData
    else:
        return convertBroSimple(inData, type, fh)

def convertBroSimple(inData, type, fh):
    bro_integer_types = ['port', 'count', 'int']
    bro_float_types = ['double', 'time', 'interval']
    """
        addr, string, subnet and enums are treated as strings
    """

    if inData == fh.unset:
        return None

    if type in bro_integer_types:
        return int(inData)

    if type in bro_float_types:
        return float(inData)

    if type == 'bool':
        if inData == 'T':
            return True
        else:
            return False

    return inData

def hook_func(row):
        try:
            row["id_orig_h"] = row.pop('id.orig_h')
        except KeyError:
            pass
        try:
            row["id_orig_p"] = row.pop('id.orig_p')
        except KeyError:
            pass
        try:
            row["id_resp_h"] = row.pop('id.resp_h')
        except KeyError:
            pass
        try:
            row["id_resp_p"] = row.pop('id.resp_p')
        except KeyError:
            pass
        try:
            row["timestamp"] = row.pop('ts')
        except KeyError:
            pass
        return row                           

def processFileContents(filename, handle):
        fh = fileHeader()
        my_list = list()
        try:
            first_line = handle.readline()
            if not first_line.startswith('#separator'):
                sys.stderr.write("File %s does not look like a bro formatted file\n" % filename)
                return

            fh.separator = binascii.unhexlify(first_line.split()[1][2:]).decode("utf-8") 
            sts = filename.split(".")
            id_name = '_'.join(sts[:-1])
            print ("Running...")
            print (filename)
            counter = 1
            for line in handle:
                columns = line.strip().split(fh.separator)
                if columns[0][0] == '#':
                    command = columns[0][1:]
                    if command == 'fields':
                        fh.fields = columns[1:]
                    elif command == 'types':
                        fh.types = columns[1:]
                    else:
                        value = columns[1]
                        setattr(fh, command, value)
                else:
                    row = {}
                    try:
                        for i, column in enumerate(columns):
                            convertedValue = convertBroType(column, fh.types[i], fh)
                            if convertedValue is not None:
                                row[fh.fields[i]] = convertedValue
                
                        row = hook_func(row)
                        json_data = json.dumps(row)
                        my_list.append(json_data)
                        counter = counter + 1
                    except Exception as e:
                        tb = traceback.format_exc()
                        sys.stderr.write("Unable to process line in %s, skipping: %s\n%s\nOffending line: '%s'" % (filename, str(e), tb, line))
        except Exception as e:
            tb = traceback.format_exc()
            sys.stderr.write("Exception while processing %s: %s\n%s" % (filename, str(e), tb))
            return
        return my_list

def processFile(filename):
    shortname = os.path.basename(filename)
    sys.stderr.write("Processing File: %s\n" % filename)
    with open(filename) as f:
        for excluded in EXCLUDE_LIST:
            if shortname.startswith(excluded):
                return list()
        return processFileContents(shortname, f)

