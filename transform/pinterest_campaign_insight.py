class TransformException(Exception):
    pass

def transform(src:dict,date:str):
    output:dict = {}
    try:
        output['id'] = str(src['CAMPAIGN_ID'])+"_"+str(date)
    except:
        output['id'] = None
    try:
        output['date'] = date
    except:
        raise TransformException("Add date in record")
    try:
        output['CAMPAIGN_ID'] = src['CAMPAIGN_ID']
    except:
        output['CAMPAIGN_ID'] = None
    try:
        output['CAMPAIGN_NAME'] = src['CAMPAIGN_NAME']
    except:
        output['CAMPAIGN_NAME'] = None
    try:
        output['AD_ACCOUNT_ID'] = src['AD_ACCOUNT_ID']
    except:
        output['AD_ACCOUNT_ID'] = None
    try:
        output['ADVERTISER_ID'] = src['ADVERTISER_ID']
    except:
        output['ADVERTISER_ID'] = None
    try:
        output['TOTAL_IMPRESSION'] = src['TOTAL_IMPRESSION']
    except:
        output['TOTAL_IMPRESSION'] = None
    try:
        output['TOTAL_CLICKTHROUGH'] = src['TOTAL_CLICKTHROUGH']
    except:
        output['TOTAL_CLICKTHROUGH'] = None
    try:
        output['TOTAL_REPIN_RATE'] = src['TOTAL_REPIN_RATE']
    except:
        output['TOTAL_REPIN_RATE'] = None
    try:
        output['TOTAL_ENGAGEMENT'] = src['TOTAL_ENGAGEMENT']
    except:
        output['TOTAL_ENGAGEMENT'] = None
    try:
        output['TOTAL_CONVERSIONS'] = src['TOTAL_CONVERSIONS']
    except:
        output['TOTAL_CONVERSIONS'] = None
    try:
        output['SPEND_IN_DOLLAR'] = src['SPEND_IN_DOLLAR']
    except:
        output['SPEND_IN_DOLLAR'] = None

    return output