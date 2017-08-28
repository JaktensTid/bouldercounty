import csv
import os
import requests
from pymongo import MongoClient
from itertools import chain
from lxml.html import fromstring
from multiprocessing.dummy import Pool as ThreadPool

client = MongoClient(os.environ['MONGODB_URI'])
db = client.data
col = db.bouldercountyrecords

global_counter = 0
urls = []

with open('wells.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        urls.append(row['URL,C,100'])


def scrape(url):
    global global_counter
    response = requests.get(url).text
    d = fromstring(response)
    if 'We are sorry for the error. If this error persists, please notify the BasinLandRecords.com administrator.' in response:
        print('Alarm!')
    def by_id(id):
        text = d.xpath(".//*[@id='%s']/text()" % id)
        if text:
            return text[0].strip()
        else:
            return ''

    def event_dates():
        item = {}
        div_left = d.xpath(".//div[@id='wc_dates']/div[@class='left_column']")
        div_right = d.xpath(".//div[@id='wc_dates']/div[@class='right_column']")
        if not div_left and not div_right:
            return None
        div_left, div_right = div_left[0], div_right[0]
        for header, text in chain(zip([entry.replace(':', '').strip() for entry in div_left.xpath('./label/text()')],
                                [entry.strip() for entry in div_left.xpath('./label/following-sibling::text()[1]')]),
                                  zip([entry.replace(':', '').strip() for entry in div_right.xpath('./label/text()')],
                                      [entry.strip() for entry in
                                       div_right.xpath('./label/following-sibling::text()[1]')])
                                  ):
            item[header] = text
        return item


    def parse_table(table):
        table_items = []
        headers = [entry.strip() for entry in table.xpath("./thead/tr[last()]/th//text()")]
        for tr in table.xpath("./tbody/tr"):
            item = {}
            for header, content in zip(headers, [entry.strip() for entry in tr.xpath("./td//text()")]):
                text = content if content else None
                item[header] = text
            table_items.append(item)
        return table_items

    def parse_table_by_id(predicate_div_id):
        xpath = ".//div[@id='%s']//table" % predicate_div_id
        table = d.xpath(xpath)
        if not table:
            return []
        table = table[0]
        return parse_table(table)

    def parse_table_by_summary_and_id(summary, predicate_div_id):
        xpath = ".//*[@id='%s']/table[@summary='%s']" % (predicate_div_id, summary)
        table = d.xpath(xpath)
        if not table:
            return []
        table = table[0]
        return parse_table(table)

    def parse_table_by_summary(summary):
        xpath = ".//table[@summary='%s']" % summary
        table = d.xpath(xpath)
        if not table:
            return []
        table = table[0]
        return parse_table(table)

    item = {'url' : url}
    item['title'] = by_id("ctl00_ctl00__main_main_lblApi")
    item['General well information'] = {
        'Operator': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblOperator"),
        'Status': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblStatus"),
        'Well type': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblWellType"),
        'Work type': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblWorkType"),
        'Direction': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblDirectionalStatus"),
        'Multi-Lateral': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblMultiLateral"),
        'Mineral owner': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblMineralOwner"),
        'Surface owner': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblSurfaceOwner"),
        'Surface location':
            by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_Location_lblLocation") + '|' +
            by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_Location_lblFootageNSH") + '|' +
            by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_Location_lblFootageEW"),
        'LatLong': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_Location_lblCoordinates"),
        'GLElevation': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblGLElevation"),
        'KBElevation': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblKBElevation"),
        'DFElevation': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblDFElevation"),
        'SignMult Compl': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblCompletions"),
        'Potash Waiver': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblPotashWaiver"),
        'Pre-ONGARD Information': {
            'Original Well Name': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblOriginalWellName"),
            'Original Operator Name': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblOriginalOperatorName")
        },
        'Proposed Formation and/or Notes': by_id(
            "ctl00_ctl00__main_main_ucGeneralWellInformation_lblProposedFormation"),
        'Depths': {
            'Proposed': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblProposedDepth"),
            'Measured Vertical Depth': by_id(
                "ctl00_ctl00__main_main_ucGeneralWellInformation_lblMeasuredVerticalDepth"),
            'Total Vertical Depth': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblMeasuredDepth"),
            'Plugback Measured': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblPlugbackMeasuredDpth")
        },
        'Formation tops': parse_table_by_summary_and_id("Well Formation Tops", "formation_tops"),
        'Event dates': {
            'Initial APD Approval': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblApdInitialApprovalDate"),
            'Most Recent APD Approval': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblApdEffectiveDate"),
            'APD Cancellation': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblApdCancellationDate"),
            'APD Extension Approval': by_id(
                "ctl00_ctl00__main_main_ucGeneralWellInformation_lblApdExtensionApprovalEffectiveDate"),
            'Spud': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblSpudDate"),
            'Approved Temporary': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblTADate"),
            'Abandonment': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblTADate"),
            'Shut In': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblShutInWaitingForPipelineDate"),
            'Plug and Abandoned Intent': by_id(
                "ctl00_ctl00__main_main_ucGeneralWellInformation_lblPluggedAbandonedDateIntent"),
            'Received': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblPluggedAbandonedDateIntent"),
            'Well Plugged': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblPluggedDate"),
            'Site Release': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblSiteReleaseDate"),
            'Last Inspection': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblLastInspectionDate"),
            'Current APD Expiration': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblApdExpirationDate"),
            'Gas Capture Plan Received': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblGasCapturePlanDate"),
            'TA Expiration': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblTAExpirationDate"),
            'PNR Expiration': by_id(
                "ctl00_ctl00__main_main_ucGeneralWellInformation_lblPluggedNotReleasedExpirationDate"),
            'Last MIT/BHT': by_id("ctl00_ctl00__main_main_ucGeneralWellInformation_lblLastMitDate"),
        },
    }
    item['History'] = parse_table_by_summary_and_id('History', "history")
    item['Operator'] = {
        'Company': by_id("ctl00_ctl00__main_main_ucOperator_lblOgrid"),
        'Address': by_id("ctl00_ctl00__main_main_ucOperator_lblAddress"),
        'Country': by_id("ctl00_ctl00__main_main_ucOperator_lblCountry"),
        'Main phone': by_id("ctl00_ctl00__main_main_ucOperator_lblMainPhone"),
        'Central contact person': {
            'Name': by_id("ctl00_ctl00__main_main_ucOperator_lblName"),
            'Phone': by_id("ctl00_ctl00__main_main_ucOperator_lblPhone"),
            'Cell': by_id("ctl00_ctl00__main_main_ucOperator_lblCell"),
            'E-Main': by_id("ctl00_ctl00__main_main_ucOperator_lblEmail"),
            'Title': by_id("ctl00_ctl00__main_main_ucOperator_lblTitle"),
            'Fax': by_id("ctl00_ctl00__main_main_ucOperator_lblFax")
        }
    }
    item['Well completions'] = {
        'Title' : by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_lblWellInformationPool") + ' ' +
        by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_lblWellInformationPoolName"),
        'Status' : by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_lblCompletionStatus"),
        'Bottomhole location' : by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_Location_lblLocation") + '|' +
        by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_Location_lblFootageNSH") + '|' +
        by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_Location_lblFootageEW"),
        'LatLong' : by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_Location_lblCoordinates"),
        'DHC' : by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_lblDHC"),
        'Last produced' : by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_lblLastProduced"),
        'Consolidation Code' : by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_lblConsolidationCode"),
        'Production Method' : by_id("ctl00_ctl00__main_main_ucWellCompletions_rptCompletionsSummary_ctl00_lblProductionMethod"),
        'Perforations' : parse_table_by_summary_and_id("Well Formation Tops", "wc_preforations"),
        'Well completions history' : parse_table_by_summary_and_id("History", "wc_history"),
        'Event dates' : event_dates()
    }
    item['Financial Assurance'] = parse_table_by_id("AdditionalFinancialAssuranceDetails")
    item['Production\Injection'] = parse_table_by_summary("Well Production Summary")
    item['Transporters'] = parse_table_by_summary("Well Search Results")
    item['Point of disposition'] = parse_table_by_summary("Points of Disposition")
    global_counter += 1
    print(global_counter)
    col.insert_one(item)


def main():
    pool = ThreadPool(1)
    pool.map(scrape, urls)


if __name__ == '__main__':
    main()
