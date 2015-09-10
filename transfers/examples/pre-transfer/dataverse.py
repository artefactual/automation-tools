#!/usr/bin/env python
"""
Parse dataset.json and produce a METS.xml with the DDI info and a structMap with the expected transfer structure inside the research data package.
"""
from __future__ import print_function
import json
from lxml import etree
import os
import sys
import uuid

import metsrw

THIS_DIR = os.path.abspath(os.path.dirname(__file__))
DISTRBTR = 'SP Dataverse Network'
# Mapping from originalFormatLabel to file extension
# Reference for values:
# https://github.com/IQSS/dataverse/blob/4.0.1/src/main/java/MimeTypeDisplay.properties
# https://github.com/IQSS/dataverse/blob/4.0.1/src/main/java/META-INF/mime.types
EXTENSION_MAPPING = {
    'Comma Separated Values': '.csv',
    'MS Excel (XLSX)': '.xlsx',
    'R Data': '.RData',
    'SPSS Portable': '.por',
    'SPSS SAV': '.sav',
    'Stata Binary': '.dta',
    'Stata 13 Binary': '.dta',
}

def get_ddi_titl_author(j):
    titl_text = authenty_text = None
    for field in j['latestVersion']['metadataBlocks']['citation']['fields']:
        if field['typeName'] == 'title':
            titl_text = field['value']
        if field['typeName'] == 'author':
            authenty_text = field['value'][0]['authorName']['value']
    return titl_text, authenty_text

def create_ddi(j):
    """
    Create the DDI dmdSec from JSON information. Returns Element.
    """
    # Get data
    titl_text, authenty_text = get_ddi_titl_author(j)
    agency = j['protocol']
    idno = j['authority'] + '/' + j['identifier']
    version_date = j['latestVersion']['releaseTime']
    version_type = j['latestVersion']['versionState']
    version_num = str(j['latestVersion']['versionNumber']) + '.' + str(j['latestVersion']['versionMinorNumber'])
    restrctn_text = j['latestVersion'].get('termsOfUse')

    # create XML
    nsmap = {'ddi': 'http://www.icpsr.umich.edu/DDI'}
    ddins = '{' + nsmap['ddi'] + '}'
    ddi_root = etree.Element(ddins + 'codebook', nsmap=nsmap)
    ddi_root.attrib['version'] = '2.5'
    ddi_root.attrib['{http://www.w3.org/2001/XMLSchema-instance}schemaLocation'] = 'http://www.ddialliance.org/Specification/DDI-Codebook/2.5/XMLSchema/codebook.xsd'
    stdydscr = etree.SubElement(ddi_root, ddins + 'stdyDscr', nsmap=nsmap)
    citation = etree.SubElement(stdydscr, ddins + 'citation', nsmap=nsmap)

    titlstmt = etree.SubElement(citation, ddins + 'titlStmt', nsmap=nsmap)
    etree.SubElement(titlstmt, ddins + 'titl', nsmap=nsmap).text = titl_text
    etree.SubElement(titlstmt, ddins + 'IDNo', agency=agency).text = idno

    rspstmt = etree.SubElement(citation, ddins + 'rspStmt')
    etree.SubElement(rspstmt, ddins + 'AuthEnty').text = authenty_text

    diststmt = etree.SubElement(citation, ddins + 'distStmt')
    etree.SubElement(diststmt, ddins + 'distrbtr').text = DISTRBTR

    verstmt = etree.SubElement(citation, ddins + 'verStmt')
    etree.SubElement(verstmt, ddins + 'version', date=version_date, type=version_type).text = version_num

    dataaccs = etree.SubElement(stdydscr, ddins + 'dataAccs')
    usestmt = etree.SubElement(dataaccs, ddins + 'useStmt')
    etree.SubElement(usestmt, ddins + 'restrctn').text = restrctn_text

    return ddi_root

def main(transfer_path):
    # Read JSON
    json_path = os.path.join(transfer_path, 'dataset.json')
    with open(json_path, 'r') as f:
        j = json.load(f)

    # Parse DDI into XML
    ddi_root = create_ddi(j)

    # Create METS
    sip = metsrw.FSEntry(
        path=None,
        label=get_ddi_titl_author(j)[0],
        use=None,
        type='Directory',
    )
    sip.add_dmdsec(
        md=ddi_root,
        mdtype='DDI',
    )
    sip.add_dmdsec(
        md='dataset.json',
        mdtype='OTHER',
        mode='mdref',
        label='dataset.json',
        loctype='OTHER',
        otherloctype='SYSTEM',
    )

    # Add original files
    tabfile_json = None
    for file_json in j['latestVersion']['files']:
        # TODO how to actually tell what is original file?
        if file_json['datafile']['name'].endswith('.tab'):
            tabfile_json = file_json
        else:
            f = metsrw.FSEntry(
                path=file_json['datafile']['name'],
                use='original',
                file_uuid=str(uuid.uuid4()),
                checksumtype='MD5',
                checksum=file_json['datafile']['md5'],
            )
            sip.add_child(f)

    # Add dataset.json
    f = metsrw.FSEntry(
        path='dataset.json',
        use='metadata',
        file_uuid=str(uuid.uuid4()),
    )
    sip.add_child(f)

    # If tabfile, set up bundle
    if tabfile_json:
        # Base name is .tab with suffix stripped
        base_name = tabfile_json['label'][:-4]
        bundle = metsrw.FSEntry(
            path=base_name,
            type='Directory',
        )
        sip.add_child(bundle)

        # Find original file
        ext = EXTENSION_MAPPING[tabfile_json['datafile']['originalFormatLabel']]
        original_file = metsrw.FSEntry(
            path=base_name + '/' + base_name + ext,
            use='original',
            file_uuid=str(uuid.uuid4()),
            checksumtype='MD5',
            checksum=tabfile_json['datafile']['md5']
        )
        bundle.add_child(original_file)
        if tabfile_json['datafile']['originalFormatLabel'] != "R Data":
            # RData derivative
            f = metsrw.FSEntry(
                path=base_name + '/' + base_name + '.RData',
                use='derivative',
                derived_from=original_file,
                file_uuid=str(uuid.uuid4()),
            )
            bundle.add_child(f)

        # Add expected bundle contents
        # FIXME what is the actual path for the files?
        # Tabfile
        f = metsrw.FSEntry(
            path=base_name + '/' + tabfile_json['datafile']['name'],
            use='derivative',
            derived_from=original_file,
            file_uuid=str(uuid.uuid4()),
        )
        f.add_dmdsec(
            md=base_name + '/' + base_name + '-ddi.xml',
            mdtype='DDI',
            mode='mdref',
            label=base_name + '-ddi.xml',
            loctype='OTHER',
            otherloctype='SYSTEM',
        )
        bundle.add_child(f)
        # -ddi.xml
        f = metsrw.FSEntry(
            path=base_name + '/' + base_name + '-ddi.xml',
            use='metadata',
            derived_from=original_file,
            file_uuid=str(uuid.uuid4()),
        )
        bundle.add_child(f)
        # citation - endnote
        f = metsrw.FSEntry(
            path=base_name + '/' + base_name + 'citation-endnote.xml',
            use='metadata',
            derived_from=original_file,
            file_uuid=str(uuid.uuid4()),
        )
        bundle.add_child(f)
        # citation - ris
        f = metsrw.FSEntry(
            path=base_name + '/' + base_name + 'citation-ris.ris',
            use='metadata',
            derived_from=original_file,
            file_uuid=str(uuid.uuid4()),
        )
        bundle.add_child(f)

    # Write METS
    metadata_path = os.path.join(transfer_path, 'metadata')
    if not os.path.exists(metadata_path):
        os.makedirs(metadata_path)

    mets_path = os.path.join(metadata_path, 'METS.xml')
    mets_f = metsrw.METSDocument()
    mets_f.append_file(sip)
    # print(mets_f.tostring(fully_qualified=True).decode('ascii'))
    mets_f.write(mets_path, pretty_print=True, fully_qualified=True)


if __name__ == '__main__':
    transfer_path = sys.argv[1]
    main(transfer_path)
